// Shared test fixtures for integration tests
// Re-export transport helpers for easy `use fixtures::transport::*`.

pub mod transport;

#[allow(unused_macros)]
macro_rules! define_transport_tests {
	(
		$tcp_connector:ty,
		$ws_connector:ty;
		$(
			$tcp_test_name:ident / $ws_test_name:ident => $helper_name:ident
		),+ $(,)?
	) => {
		$(
			#[tokio::test]
			async fn $tcp_test_name() {
				let server = fitz::testkit::TestServer::start().await.expect("start");
				$helper_name::<$tcp_connector>(&server).await;
			}

			#[tokio::test]
			async fn $ws_test_name() {
				let server = fitz::testkit::TestServer::start().await.expect("start");
				$helper_name::<$ws_connector>(&server).await;
			}
		)+
	};
}

#[allow(unused_imports)]
pub(crate) use define_transport_tests;

#[allow(unused_imports)]
pub use transport::*;
