use super::*;
use rmcp::model::{
    CallToolRequest, CallToolResult, ClientRequest, ReadResourceRequest, ReadResourceResult,
};
use rmcp::service::{PeerRequestOptions, RequestContext};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Default)]
struct BlockingServer {
    entered: Arc<Notify>,
    canceled: Arc<Notify>,
    release: Arc<Notify>,
    invocations: Arc<AtomicUsize>,
    completed: Arc<AtomicBool>,
}

impl BlockingServer {
    async fn wait(&self, context: RequestContext<RoleServer>) {
        self.invocations.fetch_add(1, Ordering::Relaxed);
        self.entered.notify_one();
        context.ct.cancelled().await;
        self.canceled.notify_one();
        self.release.notified().await;
        self.completed.store(true, Ordering::Release);
    }
}

impl ServerHandler for BlockingServer {
    async fn call_tool(
        &self,
        _: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        self.wait(context).await;
        Ok(CallToolResult::structured(serde_json::json!({"completed": true})).into())
    }
    async fn read_resource(
        &self,
        _: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        self.wait(context).await;
        Ok(ReadResourceResult::new(Vec::new()).into())
    }
}

#[tokio::test]
async fn should_forward_stdio_tool_cancellation_to_the_upstream_request_context() {
    // Arrange
    let request = ClientRequest::CallToolRequest(CallToolRequest::new(CallToolRequestParams::new(
        "inspect_resource_detail",
    )));

    // Act
    let canceled = forwarded_cancellation(request).await;

    // Assert
    assert!(
        canceled,
        "stdio cancellation never reached the upstream handler"
    );
}

#[tokio::test]
async fn should_forward_stdio_resource_cancellation_to_the_upstream_request_context() {
    // Arrange
    let request = ClientRequest::ReadResourceRequest(ReadResourceRequest::new(
        ReadResourceRequestParams::new("fitz://broker/v1/summary"),
    ));

    // Act
    let canceled = forwarded_cancellation(request).await;

    // Assert
    assert!(
        canceled,
        "stdio cancellation never reached the upstream handler"
    );
}

async fn forwarded_cancellation(request: ClientRequest) -> bool {
    let server = BlockingServer::default();
    let (upstream_io, remote_io) = tokio::io::duplex(8192);
    let upstream = tokio::spawn(server.clone().serve(upstream_io));
    let remote = ClientConfig::default().serve(remote_io).await.unwrap();
    let upstream = upstream.await.unwrap().unwrap();
    let proxy = StdioProxy {
        remote: remote.peer().clone(),
    };
    let (proxy_io, caller_io) = tokio::io::duplex(8192);
    let proxy = tokio::spawn(proxy.serve(proxy_io));
    let caller = ClientConfig::default().serve(caller_io).await.unwrap();
    let proxy = proxy.await.unwrap().unwrap();
    let handle = caller
        .peer()
        .send_cancellable_request(request, PeerRequestOptions::no_options())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), server.entered.notified())
        .await
        .unwrap();

    handle.cancel(Some("caller canceled".into())).await.unwrap();
    let canceled =
        tokio::time::timeout(Duration::from_millis(300), server.canceled.notified()).await;
    server.release.notify_one();
    caller.cancel().await.unwrap();
    proxy.cancel().await.unwrap();
    remote.cancel().await.unwrap();
    upstream.cancel().await.unwrap();

    canceled.is_ok()
}

#[tokio::test]
async fn should_reject_already_canceled_stdio_request_before_forwarding_new_work() {
    // Arrange
    let server = BlockingServer::default();
    let (upstream_io, remote_io) = tokio::io::duplex(8192);
    let upstream = tokio::spawn(server.clone().serve(upstream_io));
    let remote = ClientConfig::default().serve(remote_io).await.unwrap();
    let upstream = upstream.await.unwrap().unwrap();
    let proxy = StdioProxy {
        remote: remote.peer().clone(),
    };
    let context = RequestContext::new(rmcp::model::RequestId::Number(1), upstream.peer().clone());
    context.ct.cancel();

    // Act
    let response = tokio::time::timeout(
        Duration::from_millis(300),
        proxy.call_tool(
            CallToolRequestParams::new("inspect_resource_detail"),
            context,
        ),
    )
    .await;
    let invocations = server.invocations.load(Ordering::Relaxed);
    server.release.notify_one();
    remote.cancel().await.unwrap();
    upstream.cancel().await.unwrap();

    // Assert
    assert!(matches!(response, Ok(Err(_))));
    assert_eq!(invocations, 0);
}

#[tokio::test]
async fn should_report_indeterminate_confirmation_and_forward_cancellation_before_upstream_cleanup()
{
    // Arrange
    let server = BlockingServer::default();
    let (upstream_io, remote_io) = tokio::io::duplex(8192);
    let upstream = tokio::spawn(server.clone().serve(upstream_io));
    let remote = ClientConfig::default().serve(remote_io).await.unwrap();
    let upstream = upstream.await.unwrap().unwrap();
    let proxy = StdioProxy {
        remote: remote.peer().clone(),
    };
    let context = RequestContext::new(rmcp::model::RequestId::Number(1), upstream.peer().clone());
    let cancellation = context.ct.clone();
    let request = tokio::spawn(async move {
        proxy
            .call_tool(CallToolRequestParams::new("confirm_runtime_drain"), context)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), server.entered.notified())
        .await
        .unwrap();

    // Act
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), request)
        .await
        .unwrap()
        .unwrap();
    let canceled = tokio::time::timeout(Duration::from_secs(1), server.canceled.notified()).await;
    let completed_early = server.completed.load(Ordering::Acquire);
    server.release.notify_one();
    remote.cancel().await.unwrap();
    upstream.cancel().await.unwrap();

    // Assert
    let error = result.unwrap_err();
    assert!(error.message.contains("indeterminate"));
    assert!(error.message.contains("do not retry"));
    assert!(canceled.is_ok());
    assert!(!completed_early);
}
