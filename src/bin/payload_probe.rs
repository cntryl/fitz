use std::time::Instant;
use fitz::domains::notification::minimal::NotificationDomain;
use fitz::protocol::tlv::MessageType;
use std::hint::black_box;

fn main() {
    let sizes = [16usize, 64usize, 256usize];
    let subs = 64usize;
    let iters = 200_000usize;

    for &size in &sizes {
        let mut domain = NotificationDomain::new();
        for sub in 0..subs { domain.register(1, sub); }
        let payload = vec![0u8; size];
        // warmup
        for _ in 0..10_000 { let _ = domain.handle(MessageType::new(1), black_box(&payload)); }

        let start = Instant::now();
        for _ in 0..iters {
            let _ = domain.handle(MessageType::new(1), black_box(&payload));
        }
        let elapsed = start.elapsed();
        let per = elapsed.as_secs_f64() * 1e9 / (iters as f64);
        println!("size {}B: total {:?}, per-op {:.2} ns", size, elapsed, per);
    }
}
