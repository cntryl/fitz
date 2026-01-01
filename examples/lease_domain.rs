//! Example demonstrating the Lease domain for distributed locking
//!
//! This example shows:
//! - Acquiring exclusive locks with fencing tokens
//! - Idempotent acquire operations
//! - Lease renewal with token validation
//! - Lease takeover after expiration
//! - Fencing token protection against split-brain
//!
//! Run with: cargo run --example lease_domain

use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::scheduler::Scheduler;
use std::thread;
use std::time::Duration;

fn main() {
    println!("=== Lease Domain Example ===\n");

    let scheduler = Scheduler::new(1);
    scheduler.start();

    // Spawn lease actor
    let lease_actor = LeaseActor::new();
    let lease_ref = scheduler.spawn(lease_actor, 100);

    println!("Example 1: Basic Lease Acquisition");
    println!("-----------------------------------");

    // Acquire a lease
    println!("Client A: Acquiring lease 'my-lock'...");
    lease_ref
        .send(LeaseMessage::Acquire {
            lease_id: "my-lock".to_string(),
            owner_id: "client-a".to_string(),
            ttl_secs: 5,
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    // Try to acquire the same lease (should fail)
    println!("Client B: Trying to acquire same lease...");
    lease_ref
        .send(LeaseMessage::Acquire {
            lease_id: "my-lock".to_string(),
            owner_id: "client-b".to_string(),
            ttl_secs: 5,
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    // Idempotent acquire by original owner
    println!("Client A: Re-acquiring (idempotent)...");
    lease_ref
        .send(LeaseMessage::Acquire {
            lease_id: "my-lock".to_string(),
            owner_id: "client-a".to_string(),
            ttl_secs: 5,
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    println!("\nExample 2: Lease Renewal");
    println!("------------------------");

    // Simulate tracking the fencing token
    let fencing_token = 1; // From first acquisition

    println!("Client A: Renewing lease with token {}...", fencing_token);
    lease_ref
        .send(LeaseMessage::Renew {
            lease_id: "my-lock".to_string(),
            owner_id: "client-a".to_string(),
            fencing_token,
            ttl_secs: 5,
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    println!("Client B: Trying to renew with wrong owner...");
    lease_ref
        .send(LeaseMessage::Renew {
            lease_id: "my-lock".to_string(),
            owner_id: "client-b".to_string(),
            fencing_token,
            ttl_secs: 5,
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    println!("\nExample 3: Lease Release");
    println!("------------------------");

    println!("Client A: Releasing lease...");
    lease_ref
        .send(LeaseMessage::Release {
            lease_id: "my-lock".to_string(),
            owner_id: "client-a".to_string(),
            fencing_token,
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    println!("Client B: Acquiring after release...");
    lease_ref
        .send(LeaseMessage::Acquire {
            lease_id: "my-lock".to_string(),
            owner_id: "client-b".to_string(),
            ttl_secs: 5,
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    println!("\nExample 4: Expiration and Takeover");
    println!("----------------------------------");

    // Acquire with short TTL
    println!("Client A: Acquiring lease with 2-second TTL...");
    lease_ref
        .send(LeaseMessage::Acquire {
            lease_id: "expiring-lock".to_string(),
            owner_id: "client-a".to_string(),
            ttl_secs: 2,
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    println!("Waiting 3 seconds for lease to expire...");
    thread::sleep(Duration::from_secs(3));

    println!("Client B: Acquiring expired lease (takeover)...");
    lease_ref
        .send(LeaseMessage::Acquire {
            lease_id: "expiring-lock".to_string(),
            owner_id: "client-b".to_string(),
            ttl_secs: 5,
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    println!("\nExample 5: Fencing Token Protection");
    println!("------------------------------------");

    // Simulate old client trying to renew with stale token
    let old_token = 3; // From Client A's acquisition
    let _new_token = 4; // Client B got this on takeover (for documentation)

    println!("Client A: Trying to renew with stale token {}...", old_token);
    lease_ref
        .send(LeaseMessage::Renew {
            lease_id: "expiring-lock".to_string(),
            owner_id: "client-a".to_string(),
            fencing_token: old_token,
            ttl_secs: 5,
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    println!("\nExample 6: Query Lease Status");
    println!("------------------------------");

    println!("Querying 'expiring-lock' status...");
    lease_ref
        .send(LeaseMessage::Query {
            lease_id: "expiring-lock".to_string(),
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    println!("Querying non-existent lease...");
    lease_ref
        .send(LeaseMessage::Query {
            lease_id: "does-not-exist".to_string(),
        })
        .expect("Failed to send");

    thread::sleep(Duration::from_millis(100));

    println!("\n=== All examples completed ===");
    println!("\nKey Observations:");
    println!("- Exclusive ownership enforced (Client B cannot take active lease)");
    println!("- Idempotent operations (re-acquiring returns same token)");
    println!("- Fencing protection (stale tokens are rejected)");
    println!("- Expiration enables takeover (Client B can acquire expired lease)");
    println!("- Monotonic tokens (each acquisition gets higher token)");
}
