//! TCP transport using the shared FTZ session handler.

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::core::engine::EnginePool;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TcpTransport {
    pub addr: SocketAddr,
    pub engine: EnginePool,
}

impl TcpTransport {
    pub fn new(addr: SocketAddr, engine: EnginePool) -> Self {
        Self { addr, engine }
    }

    pub async fn run(self) -> tokio::io::Result<()> {
        // If TLS is configured, run TLS listener; otherwise plain TCP
        if let Some(tls) = crate::config::load().transport.tls {
            return self.run_tls(tls).await;
        }
        let listener = TcpListener::bind(self.addr).await?;
        tracing::info!("tcp listening on {}", self.addr);
        
        while let Ok((stream, peer)) = listener.accept().await {
            let engine_pool = self.engine.clone();
            tokio::spawn(async move {
                tracing::debug!("tcp connection accepted from {}", peer);
                
                // TCP requires authentication via first frame or pre-shared config
                // For now, create a default dev session if NO_AUTH is enabled
                let (route_family, session_auth) = if crate::authn::no_auth_enabled() {
                    let rf = "dev".to_string();
                    let session = crate::authz::SessionAuth {
                        subject: "tcp-client".to_string(),
                        route_family: rf.clone(),
                        scopes: vec!["*".to_string()],
                        grants: crate::authz::PermissionGrants::from_scopes(&rf, &["*".to_string()]),
                    };
                    (rf, session)
                } else {
                    tracing::warn!("tcp connection from {} rejected: authentication not implemented for TCP", peer);
                    return;
                };
                
                // Select engine shard based on route_family
                let engine = engine_pool.get_handle(&route_family);
                
                // Assign connection ID
                static NEXT_CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
                let conn_id = NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                
                // Create outbound channel (bounded for backpressure)
                let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel::<Arc<Vec<u8>>>(256);
                
                // Register session and connection with engine
                engine.register_session(conn_id, session_auth);
                engine.register_connection(conn_id, outbound_tx);

                // Split stream into read/write halves
                let (mut reader, mut writer) = stream.into_split();

                // Writer task: send frames back to client
                let writer_handle = tokio::spawn(async move {
                    while let Some(frame_bytes) = outbound_rx.recv().await {
                        if writer.write_all(&frame_bytes).await.is_err() {
                            break;
                        }
                    }
                });

                // Read loop with length-based reassembly of FTZ frames
                const MAX_PAYLOAD: usize = 16 * 1024 * 1024; // 16 MiB guard
                let mut inbuf: Vec<u8> = Vec::with_capacity(8 * 1024);
                let mut tmp = [0u8; 8192];
                loop {
                    match reader.read(&mut tmp).await {
                        Ok(0) => break, // connection closed
                        Ok(n) => {
                            inbuf.extend_from_slice(&tmp[..n]);
                            // Extract frames based on first u32 length prefix
                            loop {
                                if inbuf.len() < 4 {
                                    break;
                                }
                                let total_len =
                                    u32::from_be_bytes([inbuf[0], inbuf[1], inbuf[2], inbuf[3]])
                                        as usize;
                                if total_len == 0 || total_len > MAX_PAYLOAD + 1024 * 1024 {
                                    break;
                                }
                                if inbuf.len() < total_len {
                                    break;
                                }
                                let frame_bytes: Vec<u8> = inbuf.drain(0..total_len).collect();
                                // Send frame to engine for processing
                                // If backpressure detected (false return), close connection
                                if !engine.on_frame(conn_id, frame_bytes) {
                                    tracing::warn!("tcp {conn_id} closing due to engine backpressure");
                                    break;
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }

                // Cleanup: notify engine of disconnect
                engine.on_disconnect(conn_id);
                writer_handle.abort();
            });
        }
        Ok(())
    }

    async fn run_tls(self, tls: crate::config::TlsConfig) -> tokio::io::Result<()> {
        use tokio_rustls::rustls::ServerConfig;
        use tokio_rustls::TlsAcceptor;
        let mut config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(tls.cert_chain, tls.priv_key)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad certs"))?;
        // Configure ALPN for websockets over TLS if desired (optional)
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let acceptor = TlsAcceptor::from(std::sync::Arc::new(config));
        let listener = TcpListener::bind(self.addr).await?;
        while let Ok((stream, _peer)) = listener.accept().await {
            let acceptor = acceptor.clone();
            let engine_pool = self.engine.clone();
            tokio::spawn(async move {
                match acceptor.accept(stream).await {
                    Ok(tls_stream) => {
                        // Create default session for NO_AUTH mode
                        let (route_family, session_auth) = if crate::authn::no_auth_enabled() {
                            let rf = "dev".to_string();
                            let session = crate::authz::SessionAuth {
                                subject: "tls-client".to_string(),
                                route_family: rf.clone(),
                                scopes: vec!["*".to_string()],
                                grants: crate::authz::PermissionGrants::from_scopes(&rf, &["*".to_string()]),
                            };
                            (rf, session)
                        } else {
                            tracing::warn!("tls connection rejected: authentication not implemented for TLS");
                            return;
                        };
                        
                        // Select engine shard based on route_family
                        let engine = engine_pool.get_handle(&route_family);
                        
                        // Assign connection ID
                        static NEXT_CONN_ID_TLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1000000);
                        let conn_id = NEXT_CONN_ID_TLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        
                        // Create outbound channel (bounded for backpressure)
                        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel::<Arc<Vec<u8>>>(256);
                        
                        // Register session and connection
                        engine.register_session(conn_id, session_auth);
                        engine.register_connection(conn_id, outbound_tx);
                        
                        // Split TLS stream
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let (mut reader, mut writer) = tokio::io::split(tls_stream);
                        
                        let writer_handle = tokio::spawn(async move {
                            while let Some(frame_bytes) = outbound_rx.recv().await {
                                if writer.write_all(&frame_bytes).await.is_err() {
                                    break;
                                }
                            }
                        });
                        const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
                        let mut inbuf: Vec<u8> = Vec::with_capacity(8 * 1024);
                        let mut tmp = [0u8; 8192];
                        loop {
                            match reader.read(&mut tmp).await {
                                Ok(0) => break,
                                Ok(n) => {
                                    inbuf.extend_from_slice(&tmp[..n]);
                                    loop {
                                        if inbuf.len() < 4 {
                                            break;
                                        }
                                        let total_len = u32::from_be_bytes([
                                            inbuf[0], inbuf[1], inbuf[2], inbuf[3],
                                        ])
                                            as usize;
                                        if total_len == 0 || total_len > MAX_PAYLOAD + 1024 * 1024 {
                                            break;
                                        }
                                        if inbuf.len() < total_len {
                                            break;
                                        }
                                        let frame_bytes: Vec<u8> =
                                            inbuf.drain(0..total_len).collect();
                                        // Send frame to engine for processing
                                        // If backpressure detected (false return), close connection
                                        if !engine.on_frame(conn_id, frame_bytes) {
                                            tracing::warn!("tcp tls {conn_id} closing due to engine backpressure");
                                            break;
                                        }
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        // Cleanup: notify engine of disconnect
                        engine.on_disconnect(conn_id);
                        writer_handle.abort();
                    }
                    Err(e) => {
                        tracing::error!("tls accept failed: {}", e);
                    }
                }
            });
        }
        Ok(())
    }
}
