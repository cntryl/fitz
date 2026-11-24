# Benchmarks

Fitz tracks benchmark scenarios by workload tier. Run a target directly with
`cargo bench -p <package> --bench <target>` and compare reports from the same target and
environment. Use `cntryl-tools summarize-benchmarks` when a report summary is
needed. Benchmark results describe only the measured source and environment;
they are not product performance claims.

The benchmark workflows are diagnostic and are not a pull-request performance
gate. See [bench targets](bench-targets.md) for the workload inventory and
[cloud catalog recovery](cloud-catalog-pressure.md) for its external-provider
requirements.

Routing and framing benchmarks belong to `fitz-core`. The permissions
benchmark belongs to `fitz`. Direct actor and store workloads belong to
`fitz-kv`, `fitz-schedule`, and `fitz-stream`; broker dispatch and transport
workloads belong to `fitz-e2e`. Use `cargo metadata --no-deps --format-version 1`
to locate the package for a target.
