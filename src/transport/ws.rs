// Clean, minimal ws.rs implementation that defers protocol handling to session.rs

use crate::core::engine::EngineHandle;
use futures::sink::SinkExt;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::WebSocketStream;

#[derive(Clone)]
pub struct WsTransport {
    pub addr: SocketAddr,
    pub engine: EngineHandle,
}

impl WsTransport {
    pub fn new(addr: SocketAddr, engine: EngineHandle) -> Self {
        Self { addr, engine }
    }

    pub async fn run(self) -> tokio::io::Result<()> {
        crate::transport::mark_started();
        let listener = TcpListener::bind(self.addr).await?;
        crate::transport::mark_live();
        crate::transport::mark_ready();

        while let Ok((stream, _)) = listener.accept().await {
            let engine_for_task = self.engine.clone();
            tokio::spawn(async move {
                // If TLS is configured, accept TLS first, then websocket
                if let Some(tls) = crate::config::load().transport.tls {
                    use tokio_rustls::rustls::ServerConfig;
                    use tokio_rustls::TlsAcceptor;
                    let mut config = ServerConfig::builder()
                        .with_safe_defaults()
                        .with_no_client_auth()
                        .with_single_cert(tls.cert_chain, tls.priv_key)
                        .expect("invalid TLS config");
                    config.alpn_protocols = vec![b"http/1.1".to_vec()];
                    let acceptor = TlsAcceptor::from(std::sync::Arc::new(config));
                    match acceptor.accept(stream).await {
                        Ok(tls_stream) => match accept_async(tls_stream).await {
                            Ok(ws) => {
                                if let Err(e) = process_ws_stream(ws, engine_for_task).await {
                                    eprintln!("ws session error: {}", e);
                                }
                            }
                            Err(e) => eprintln!("ws accept error: {}", e),
                        },
                        Err(e) => eprintln!("tls accept error: {}", e),
                    }
                } else {
                    match accept_async(stream).await {
                        Ok(ws) => {
                            if let Err(e) = process_ws_stream(ws, engine_for_task).await {
                                eprintln!("ws session error: {}", e);
                            }
                        }
                        Err(e) => eprintln!("ws accept error: {}", e),
                    }
                }
            });
        }

        Ok(())
    }
}

pub async fn process_ws_stream<S>(
    _ws_stream: WebSocketStream<S>,
    engine: EngineHandle,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    use futures::StreamExt;

    crate::transport::inc_active_connections();

    // writer queue for outgoing frames
    let (writer_tx, mut writer_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
    let mux = crate::transport::mux::Muxer::new(writer_tx.clone());

    // split into sink and stream for independent read/write
    let (mut ws_sink, mut ws_stream) = _ws_stream.split();

    // spawn writer task
    let _write_task = tokio::spawn(async move {
        while let Some(frame_bytes) = writer_rx.recv().await {
            let _ = ws_sink
                .send(tungstenite::Message::Binary(frame_bytes))
                .await;
        }
    });

    // Register default channel on this mux; protocol logic handled in session
    crate::transport::session::register_default_channel(mux.clone(), engine.clone(), 1).await;

    // Read frames from websocket and hand off to mux for demux
    while let Some(msg) = ws_stream.next().await {
        let msg = msg?;
        if msg.is_binary() {
            let bin = msg.into_data();
            mux.demux_incoming(bin).await;
        } else if msg.is_close() {
            break;
        }
    }

    // Cleanup on disconnect - handled at session layer with proper context
    // (channel_id and route_family are only available in session, not transport)
    crate::transport::dec_active_connections();
    Ok(())
}
