mod kv;
mod lease;
mod notice;
mod queue;
mod rpc;
mod schedule;
mod stream;

use crate::boot::Runtime;

pub(super) fn append_domain_metrics(output: &mut String, runtime: &Runtime) {
    kv::append_metrics(output, runtime);
    notice::append_metrics(output, runtime);
    queue::append_metrics(output, runtime);
    rpc::append_metrics(output, runtime);
    lease::append_metrics(output, runtime);
    stream::append_metrics(output, runtime);
    schedule::append_metrics(output, runtime);
}
