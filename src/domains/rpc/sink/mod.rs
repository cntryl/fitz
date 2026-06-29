mod domain_sink_impl;
mod mailbox_adapter;
mod mailbox_sink_impl;
mod state_model;

pub use state_model::RpcDomainSink;

#[cfg(test)]
mod tests;
