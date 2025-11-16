//! TCP transport using the shared FTZ session handler.

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::core::engine::EngineHandle;

#[derive(Debug, Clone)]
pub struct TcpTransport {
    pub addr: SocketAddr,
    pub engine: EngineHandle,
}

impl TcpTransport {
    pub fn new(addr: SocketAddr, engine: EngineHandle) -> Self {
        Self { addr, engine }
    }

    pub async fn run(self) -> tokio::io::Result<()> {
        // If TLS is configured, run TLS listener; otherwise plain TCP
        if let Some(tls) = crate::config::load().transport.tls {
            return self.run_tls(tls).await;
        }
        let listener = TcpListener::bind(self.addr).await?;
        while let Ok((stream, _peer)) = listener.accept().await {
            let engine = self.engine.clone();
            tokio::spawn(async move {
                // For TCP, we use the same mux model with a writer queue
                let (writer_tx, mut writer_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
                let mux = crate::transport::mux::Muxer::new(writer_tx.clone());

                // Split stream into read/write halves
                let (mut reader, mut writer) = stream.into_split();

                // Writer task: send FTZ frames back to client
                tokio::spawn(async move {
                    while let Some(frame_bytes) = writer_rx.recv().await {
                        // Write full frame bytes
                        if writer.write_all(&frame_bytes).await.is_err() {
                            break;
                        }
                    }
                });

                // Register default channel on mux
                crate::transport::session::register_default_channel(mux.clone(), engine.clone(), 1)
                    .await;

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
                                mux.demux_incoming(frame_bytes).await;
                            }
                        }
                        Err(_) => break,
                    }
                }

                // Cleanup handled at session layer with proper channel_id and route_family context
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
            let engine = self.engine.clone();
            tokio::spawn(async move {
                match acceptor.accept(stream).await {
                    Ok(tls_stream) => {
                        // Writer queue and mux same as plain TCP path
                        let (writer_tx, mut writer_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
                        let mux = crate::transport::mux::Muxer::new(writer_tx.clone());
                        // Split TLS stream
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let (mut reader, mut writer) = tokio::io::split(tls_stream);
                        tokio::spawn(async move {
                            while let Some(frame_bytes) = writer_rx.recv().await {
                                if writer.write_all(&frame_bytes).await.is_err() {
                                    break;
                                }
                            }
                        });
                        crate::transport::session::register_default_channel(
                            mux.clone(),
                            engine.clone(),
                            1,
                        )
                        .await;
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
                                            return;
                                        }
                                        if inbuf.len() < total_len {
                                            break;
                                        }
                                        let frame_bytes: Vec<u8> =
                                            inbuf.drain(0..total_len).collect();
                                        mux.demux_incoming(frame_bytes).await;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        // Cleanup handled at session layer with proper channel_id and route_family context
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
