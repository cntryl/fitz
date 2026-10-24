//! The negotiated session boundary is identical on TCP and WebSocket.

use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::protocol::wire2::header::{decode_message, encode_message, HeaderFlags};
use fitz::testkit::transport::build_connect_frame;
use fitz::testkit::TestServer;
use futures_util::{SinkExt, StreamExt};
use std::num::NonZeroU64;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

enum WireClient {
    Tcp(TcpStream),
    WebSocket(Box<WebSocketStream<MaybeTlsStream<TcpStream>>>),
}

impl WireClient {
    async fn open(server: &TestServer, websocket: bool) -> Self {
        if websocket {
            let (stream, _) = tokio_tungstenite::connect_async(format!("ws://{}/", server.ws_addr))
                .await
                .unwrap();
            Self::WebSocket(Box::new(stream))
        } else {
            Self::Tcp(TcpStream::connect(server.tcp_addr).await.unwrap())
        }
    }

    async fn send(&mut self, frame: &[u8]) {
        match self {
            Self::Tcp(stream) => {
                stream
                    .write_u32(u32::try_from(frame.len()).unwrap())
                    .await
                    .unwrap();
                stream.write_all(frame).await.unwrap();
            }
            Self::WebSocket(stream) => stream
                .send(Message::Binary(frame.to_vec().into()))
                .await
                .unwrap(),
        }
    }

    async fn receive(&mut self) -> Result<Vec<u8>, String> {
        tokio::time::timeout(Duration::from_secs(2), async {
            match self {
                Self::Tcp(stream) => {
                    let len = stream.read_u32().await.map_err(|e| e.to_string())?;
                    let mut frame = vec![0; usize::try_from(len).unwrap()];
                    stream
                        .read_exact(&mut frame)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(frame)
                }
                Self::WebSocket(stream) => loop {
                    match stream.next().await {
                        Some(Ok(Message::Binary(frame))) => break Ok(frame.to_vec()),
                        Some(Ok(Message::Ping(body))) => {
                            stream
                                .send(Message::Pong(body))
                                .await
                                .map_err(|e| e.to_string())?;
                        }
                        Some(Ok(Message::Close(_))) | None => break Err("closed".to_owned()),
                        Some(Err(error)) => break Err(error.to_string()),
                        _ => {}
                    }
                },
            }
        })
        .await
        .map_err(|error| error.to_string())?
    }

    async fn connect(&mut self) {
        self.send(&build_connect_frame("", "")).await;
        let hello = self.receive().await.unwrap();
        assert_eq!(decode_message(&hello, u32::MAX).unwrap().0.message_type, 2);
    }
}

fn begin(correlation: u64) -> Vec<u8> {
    let mut payload = PayloadEncoder::new();
    payload.put_string("kv://acme/app/users");
    payload.put_u8(1);
    encode_message(
        100,
        HeaderFlags::none(),
        NonZeroU64::new(correlation),
        &payload.finish(),
    )
}

#[tokio::test]
async fn should_refuse_domain_execution_before_connect_on_both_transports() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    for websocket in [false, true] {
        let mut client = WireClient::open(&server, websocket).await;
        // Act
        client.send(&begin(7)).await;
        let response = client.receive().await;
        // Assert
        assert!(
            response.is_err(),
            "domain request executed before negotiation: {response:?}"
        );
    }
}

#[tokio::test]
async fn should_refuse_uncorrelated_requests_on_both_transports() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    for websocket in [false, true] {
        let mut client = WireClient::open(&server, websocket).await;
        client.connect().await;
        // Act
        client.send(&begin(0)).await;
        let response = client.receive().await;
        // Assert
        assert!(
            response.is_err(),
            "zero-correlated request was accepted: {response:?}"
        );
    }
}

#[tokio::test]
async fn should_answer_repeated_connect_with_rejection_on_both_transports() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    for websocket in [false, true] {
        let mut client = WireClient::open(&server, websocket).await;
        client.connect().await;
        // Act
        client.send(&build_connect_frame("", "")).await;
        let response = client.receive().await.unwrap();
        // Assert
        assert_eq!(
            decode_message(&response, u32::MAX).unwrap().0.message_type,
            3
        );
    }
}
