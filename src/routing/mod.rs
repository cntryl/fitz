//! Shared routing infrastructure for pub/sub and RPC patterns

mod route_table;

pub use route_table::{RouteFamilyId, RouteTable, RtSubscription, DEFAULT_RF};
