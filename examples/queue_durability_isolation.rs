//! Queue Durability Isolation Example
//!
//! Demonstrates how queue durability policies use per-write options to achieve
//! domain isolation on a shared Midge instance.
//!
//! # Architecture
//!
//! - Single shared `Arc<MidgeEngine>` for all domains
//! - Each queue actor has its own durability policy
//! - Policy translated to Midge WriteOptions at write time
//! - No global configuration changes (other domains unaffected)
//!
//! # Run Example
//!
//! ```bash
//! cargo run --example queue_durability_isolation
//! ```

use std::sync::Arc;

fn main() {
    println!("=== Queue Durability Isolation ===\n");
    
    // Single shared Midge instance for ALL domains
    println!("1. Creating shared Midge instance...");
    let shared_store = Arc::new(
        cntryl_midge::MidgeEngine::open(cntryl_midge::MidgeOptions::default())
            .expect("Failed to create Midge instance")
    );
    println!("   ✓ Midge instance created (shared by all domains)\n");
    
    // Different queue actors with different durability policies
    println!("2. Creating queue actors with different durability policies...");
    
    // Financial transactions: Strict (never lose data)
    let payments_durability = fitz::domains::queue::QueueDurabilityPolicy::Strict;
    println!("   - Payments queue: {:?}", payments_durability);
    println!("     Midge options: {:?}", payments_durability.to_midge_options());
    println!("     (sync=true, disable_wal=false) → fsync on every write\n");
    
    // Background jobs: Grouped (tolerate 5ms loss window)
    let jobs_durability = fitz::domains::queue::QueueDurabilityPolicy::Grouped { interval_ms: 5 };
    println!("   - Jobs queue: {:?}", jobs_durability);
    println!("     Midge options: {:?}", jobs_durability.to_midge_options());
    println!("     (sync=false, disable_wal=false) → async WAL, group commit\n");
    
    // Analytics events: Async (best-effort)
    let analytics_durability = fitz::domains::queue::QueueDurabilityPolicy::Async;
    println!("   - Analytics queue: {:?}", analytics_durability);
    println!("     Midge options: {:?}", analytics_durability.to_midge_options());
    println!("     (sync=false, disable_wal=true) → memory-only, no WAL\n");
    
    // Domain isolation: KV and Streams maintain Strict durability
    println!("3. Other domains maintain their own durability...");
    println!("   - KV store: Always Strict (unaffected by queue policies)");
    println!("   - Streams: Always Strict (unaffected by queue policies)");
    println!("   - Leases: Always Strict (unaffected by queue policies)\n");
    
    // Per-write override pattern (pseudocode until Midge API available)
    println!("4. Per-write override pattern:\n");
    println!("   ```rust");
    println!("   // Payments queue (Strict policy)");
    println!("   let (sync, disable_wal) = payments_policy.to_midge_options();");
    println!("   let mut opts = WriteOptions::default();");
    println!("   opts.set_sync(sync);           // true");
    println!("   opts.set_disable_wal(disable_wal); // false");
    println!("   store.put_opt(cf, &key, &value, &opts)?; // Durable write");
    println!();
    println!("   // Analytics queue (Async policy)");
    println!("   let (sync, disable_wal) = analytics_policy.to_midge_options();");
    println!("   let mut opts = WriteOptions::default();");
    println!("   opts.set_sync(sync);           // false");
    println!("   opts.set_disable_wal(disable_wal); // true");
    println!("   store.put_opt(cf, &key, &value, &opts)?; // Fast write");
    println!();
    println!("   // KV domain (always Strict, different key prefix)");
    println!("   let mut opts = WriteOptions::default();");
    println!("   opts.set_sync(true);           // Always true");
    println!("   opts.set_disable_wal(false);   // Always false");
    println!("   store.put_opt(cf, &kv_key, &value, &opts)?; // Unaffected!");
    println!("   ```\n");
    
    println!("5. Isolation guarantees:");
    println!("   ✓ Single shared Midge instance (efficient)");
    println!("   ✓ Per-write durability control (flexible)");
    println!("   ✓ No global config changes (safe)");
    println!("   ✓ Domain isolation via key prefixes + write options");
    println!("   ✓ Queue 'Async' policy won't affect KV/streams\n");
    
    println!("=== Example Complete ===");
}
