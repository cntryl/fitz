use super::super::{ack, connect, extract_queue_messages, request};
use crate::fixtures::transport::{build_queue_dequeue, build_queue_enqueue};
use std::collections::HashMap;
use std::net::SocketAddr;

const ROUTE: &str = "queue://test/s3/backlog";

fn body(index: u32) -> Vec<u8> {
    let mut state = u64::from(index) + 1;
    let mut value = vec![0; 16 * 1024];
    for byte in &mut value {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state.to_le_bytes()[0];
    }
    value[..4].copy_from_slice(&index.to_be_bytes());
    value
}

pub(super) async fn enqueue(address: SocketAddr, count: u32, first: u32) -> HashMap<u64, u32> {
    let mut producers = tokio::task::JoinSet::new();
    for worker in 0..16 {
        producers.spawn(async move {
            let mut client = connect(address).await;
            let mut accepted = Vec::new();
            for index in (first + worker..first + count).step_by(16) {
                let payload =
                    request(&mut client, &build_queue_enqueue(ROUTE, &body(index)), 200).await;
                let id = u64::from_be_bytes(payload[1..9].try_into().unwrap());
                accepted.push((id, index));
            }
            accepted
        });
    }
    let mut accepted = HashMap::new();
    while let Some(result) = producers.join_next().await {
        for (id, index) in result.unwrap() {
            assert!(accepted.insert(id, index).is_none(), "unique accepted ID");
        }
    }
    assert_eq!(accepted.len(), usize::try_from(count).unwrap());
    println!("S3 accepted {} strict 16KiB messages", accepted.len());
    accepted
}

pub(super) async fn drain(address: SocketAddr, mut accepted: HashMap<u64, u32>) {
    let mut client = connect(address).await;
    while !accepted.is_empty() {
        let reserved = request(&mut client, &build_queue_dequeue(ROUTE), 202).await;
        let id = u64::from_be_bytes(reserved[5..13].try_into().unwrap());
        let index = accepted
            .remove(&id)
            .expect("unique, acknowledged message ID");
        assert_eq!(
            extract_queue_messages(&reserved).unwrap(),
            vec![body(index)]
        );
        ack(&mut client, ROUTE, &reserved).await;
        if accepted.len().is_multiple_of(1024) {
            println!("S3 remaining={}", accepted.len());
        }
    }
    let empty = request(&mut client, &build_queue_dequeue(ROUTE), 202).await;
    assert_eq!(
        extract_queue_messages(&empty).unwrap(),
        Vec::<Vec<u8>>::new()
    );
}
