# SOLID Remediation Benchmark Report

Date: 2026-09-09
Baseline: `5ab11d8e`
Profile: `cntryl-stress` default, three runs per row, same host

This report records the representative WebSocket regression-gate row for each
domain affected by the family-ownership remediation. Throughput is the median
of the three per-run throughput medians. Latency is the median of the three
per-run p95 values. Queue's canonical lifecycle row does not publish a paired
latency measurement.

| Domain | Baseline throughput | Current throughput | Throughput ratio | Baseline p95 | Current p95 | p95 ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| KV | 6,888.53/s | 8,279.99/s | 120.2% | 206.79 us | 157.96 us | 76.4% |
| Queue | 7,075.09/s | 7,416.73/s | 104.8% | n/a | n/a | n/a |
| Notice | 29,356.48/s | 27,389.29/s | 93.3% | 49.38 us | 52.79 us | 106.9% |
| RPC | 12,742.87/s | 14,634.62/s | 114.8% | 111.33 us | 116.58 us | 104.7% |
| Lease | 14,780.98/s | 14,955.46/s | 101.2% | 85.88 us | 90.25 us | 105.1% |
| Schedule | 8,117.95/s | 9,025.19/s | 111.2% | 366.50 us | 247.50 us | 67.5% |
| Stream | 27,329.74/s | 29,654.57/s | 108.5% | 60.92 us | 51.17 us | 84.0% |

Every throughput ratio is at least 90%, and every available p95 ratio is no
more than 110%. Stream also clears its stricter 95% throughput and 105% p95
limits.

During validation, broad sequential runs showed simultaneous high-variance
outliers across unrelated domains. Lease and Schedule were therefore rerun by
alternating the remediated tree with the immutable baseline worktree; Stream
was rerun alone. The table uses those matched or isolated three-run sets rather
than treating host scheduling noise as product signal.
