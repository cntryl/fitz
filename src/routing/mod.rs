//! Shared routing infrastructure for pub/sub and RPC patterns

mod intern;
mod route_table;

pub use intern::{GlobalInternTable, InternId};
pub use route_table::{RouteFamilyId, RouteTable, RtSubscription, DEFAULT_RF};
