//! Hotpath microbenchmarks for lease internals.
//!
//! These target pure, synchronous logic only:
//! - token generation
//! - UUID formatting
//! - LeaseEntry state transitions

use base64::{engine::general_purpose, Engine as _};
use criterion::{criterion_group, criterion_main, Criterion};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;
use std::time::{Duration, Instant};

#[path = "../config.rs"]
mod config;

fn bench_token_generation(c: &mut Criterion) {
    // Recreate minimal token generation hotpath (HMAC-SHA256 + base64) so
    // this bench can run without accessing service internals.
    let key = "lease://realm/area/resource";
    let id = "123e4567-e89b-12d3-a456-426614174000".to_string();
    let expiry = Instant::now() + Duration::from_secs(30);
    let secret = b"bench-secret-key-0123456789".to_vec();

    c.bench_function("lease_token_generation", |b| {
        b.iter(|| {
            let expiry_unix = (std::time::SystemTime::now()
                + expiry.saturating_duration_since(Instant::now()))
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

            let mut mac = HmacSha256::new_from_slice(&secret).expect("HMAC key");
            mac.update(key.as_bytes());
            mac.update(b"|");
            mac.update(id.as_bytes());
            mac.update(b"|");

            // write expiry digits without alloc
            let mut buf = [0u8; 20];
            let mut t = expiry_unix;
            let mut len = 0;
            if t == 0 {
                buf[0] = b'0';
                len = 1;
            } else {
                while t > 0 {
                    buf[len] = b'0' + (t % 10) as u8;
                    t /= 10;
                    len += 1;
                }
                buf[..len].reverse();
            }
            mac.update(&buf[..len]);
            let token = general_purpose::STANDARD.encode(mac.finalize().into_bytes());
            std::hint::black_box(token);
        });
    });
}

fn bench_uuid_formatting(c: &mut Criterion) {
    c.bench_function("lease_uuid_formatting", |b| {
        b.iter(|| {
            let _ = Uuid::new_v4().to_string();
        });
    });
}

fn bench_lease_entry_state_transitions(c: &mut Criterion) {
    struct LocalLeaseEntry {
        id: String,
        token: String,
        expiry: Instant,
    }

    impl LocalLeaseEntry {
        fn free() -> Self {
            Self {
                id: String::new(),
                token: String::new(),
                expiry: Instant::now(),
            }
        }

        #[inline]
        fn is_active(&self, now: Instant) -> bool {
            !self.id.is_empty() && now < self.expiry
        }
    }

    let mut entry = LocalLeaseEntry::free();
    let now = Instant::now();

    c.bench_function("lease_entry_state_transitions", |b| {
        b.iter(|| {
            // simulate acquire
            entry.id = "id".to_string();
            entry.token = "token".to_string();
            entry.expiry = now + Duration::from_secs(30);

            // check active
            let _active = entry.is_active(now);

            // simulate expire/reset
            entry = LocalLeaseEntry::free();
        });
    });
}

criterion_group!(
    name = hotpath_lease_core;
    config = config::criterion_config();
    targets =
        bench_token_generation,
        bench_uuid_formatting,
        bench_lease_entry_state_transitions,
);

criterion_main!(hotpath_lease_core);
