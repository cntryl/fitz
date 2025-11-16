use std::convert::Infallible;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Initialize subsystems
    fitz::authz::init();
    fitz::storage::init();
    fitz::transport::init();

    // bind address
    let addr = ([0, 0, 0, 0], 8080).into();

    let make_svc = make_service_fn(|_conn| async {
        Ok::<_, Infallible>(service_fn(|_req: Request<Body>| async move {
            Ok::<_, Infallible>(Response::new(Body::from("Fitz: hello")))
        }))
    });

    let server = Server::bind(&addr).serve(make_svc);

    println!("listening on http://{}", addr);

    server.await?;
    // Keep running until ctrl-c
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");
    Ok(())
}
