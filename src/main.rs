//! Fitz main entry point - starts engine shards and transports

use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()),
        )
        .init();

    tracing::info!("Fitz starting...");

    // Initialize subsystems
    fitz::authz::init();
    fitz::storage::init();

    // Start engine pool with sharded processing
    tracing::info!("starting engine shards...");
    let engine_pool = fitz::core::engine::start_engine_pool();
    tracing::info!(
        "engine pool started with {} shards",
        fitz::core::engine::NUM_SHARDS
    );

    // Load config
    let config = fitz::config::load();

    // Start transports
    // HTTP handles both /healthz and /connect (WS upgrade) on ws_port
    let http_addr: SocketAddr = format!("0.0.0.0:{}", config.transport.ws_port)
        .parse()
        .expect("invalid HTTP address");
    let tcp_addr: SocketAddr = format!("0.0.0.0:{}", config.transport.tcp_port)
        .parse()
        .expect("invalid TCP address");

    // HTTP transport (includes /connect for WS upgrade, /healthz, /rpc/sys/token/issue)
    let http_transport = fitz::transport::http::HttpTransport::new(http_addr, engine_pool.clone());
    let http_handle = tokio::spawn(async move {
        if let Err(e) = http_transport.run().await {
            tracing::error!("HTTP transport error: {}", e);
        }
    });

    // TCP transport (raw FTZ framing)
    let tcp_transport = fitz::transport::tcp::TcpTransport::new(tcp_addr, engine_pool.clone());
    let tcp_handle = tokio::spawn(async move {
        if let Err(e) = tcp_transport.run().await {
            tracing::error!("TCP transport error: {}", e);
        }
    });

    tracing::info!("Fitz ready:");
    tracing::info!(
        "  HTTP/WS:  http://{} (includes /connect for WS upgrade)",
        http_addr
    );
    tracing::info!("  TCP:      tcp://{}", tcp_addr);
    tracing::info!("  Shards:   {}", fitz::core::engine::NUM_SHARDS);

    // Wait for Ctrl+C
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");

    tracing::info!("Shutting down...");

    // Abort transport tasks (will trigger disconnects)
    http_handle.abort();
    tcp_handle.abort();

    // Give connections time to clean up
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    tracing::info!("Fitz stopped");
    Ok(())
}
