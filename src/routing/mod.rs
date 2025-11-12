//! Shared routing infrastructure for pub/sub and RPC patterns

mod route_table;

pub use route_table::{RouteTable, RtSubscription, RouteFamilyId, DEFAULT_RF};

