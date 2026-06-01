//! Shared domain inventory used by boot registration and ingress cleanup.

use crate::runtime::routing::Route;
use once_cell::sync::Lazy;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DomainKind {
    Kv,
    Queue,
    Rpc,
    Lease,
    Notice,
    Stream,
    Schedule,
}

impl DomainKind {
    pub const ALL: [Self; 7] = [
        Self::Kv,
        Self::Queue,
        Self::Notice,
        Self::Stream,
        Self::Rpc,
        Self::Lease,
        Self::Schedule,
    ];

    pub const SESSION_CLEANUP_ORDER: [Self; 7] = [
        Self::Kv,
        Self::Notice,
        Self::Rpc,
        Self::Stream,
        Self::Schedule,
        Self::Lease,
        Self::Queue,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Kv => "kv",
            Self::Queue => "queue",
            Self::Rpc => "rpc",
            Self::Lease => "lease",
            Self::Notice => "notice",
            Self::Stream => "stream",
            Self::Schedule => "schedule",
        }
    }

    pub const fn wildcard_route(self) -> &'static str {
        match self {
            Self::Kv => "kv://**",
            Self::Queue => "queue://**",
            Self::Rpc => "rpc://**",
            Self::Lease => "lease://**",
            Self::Notice => "notice://**",
            Self::Stream => "stream://**",
            Self::Schedule => "schedule://**",
        }
    }

    pub fn cleanup_route(self) -> Route {
        Route::new(match self {
            Self::Kv => "kv://cleanup",
            Self::Queue => "queue://cleanup",
            Self::Rpc => "rpc://cleanup",
            Self::Lease => "lease://cleanup",
            Self::Notice => "notice://cleanup",
            Self::Stream => "stream://cleanup",
            Self::Schedule => "schedule://cleanup",
        })
    }

    pub fn inbound_route(self) -> &'static Route {
        static KV: Lazy<Route> = Lazy::new(|| Route::new("kv://inbound"));
        static QUEUE: Lazy<Route> = Lazy::new(|| Route::new("queue://inbound"));
        static RPC: Lazy<Route> = Lazy::new(|| Route::new("rpc://inbound"));
        static LEASE: Lazy<Route> = Lazy::new(|| Route::new("lease://inbound"));
        static NOTICE: Lazy<Route> = Lazy::new(|| Route::new("notice://inbound"));
        static STREAM: Lazy<Route> = Lazy::new(|| Route::new("stream://inbound"));
        static SCHEDULE: Lazy<Route> = Lazy::new(|| Route::new("schedule://inbound"));

        match self {
            Self::Kv => &KV,
            Self::Queue => &QUEUE,
            Self::Rpc => &RPC,
            Self::Lease => &LEASE,
            Self::Notice => &NOTICE,
            Self::Stream => &STREAM,
            Self::Schedule => &SCHEDULE,
        }
    }
}
