# Fitz-Go Client Performance Documentation

## Phase 0: Baseline Established

### Infrastructure Created

- **Configuration Framework**: `internal/config/` with env var overrides
- **Benchmark Baseline System**: `internal/benchkit/` for tracking performance
- **Buffer Pool Audit**: `scripts/audit_buffer_pool.go` for analyzing buffer usage
- **Test Consolidation**: Removed duplicate `internal/testhelpers/`, using `internal/testkit/` only

### Configuration Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `FITZ_POOL_SIZE` | 1024 | Initial buffer pool size |
| `FITZ_POOL_MAX` | 10000 | Maximum pooled buffers |
| `FITZ_POOL_MAX_BUFFER` | 65536 | Max buffer size (64KB) |
| `FITZ_BATCH_SIZE` | 100 | Max requests per batch |
| `FITZ_BATCH_TIMEOUT` | 10ms | Batch flush interval |
| `FITZ_BACKPRESSURE` | true | Enable queue backpressure |
| `FITZ_CPU_PROFILE` | false | Enable CPU profiling |
| `FITZ_MEM_PROFILE` | false | Enable memory profiling |

### Next Steps

See the optimization plan for Phase 1 (Quick Wins) implementation.

**Status**: Phase 0 complete. Ready for Phase 1.
