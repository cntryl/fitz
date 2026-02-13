package benchkit

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Histogram collects latency measurements and computes percentiles.
type Histogram struct {
	mu     sync.Mutex
	values []int64 // latencies in nanoseconds, sorted
	count  int64
	sum    int64
	minVal int64
	maxVal int64
	sorted bool
}

// NewHistogram creates a new latency histogram.
func NewHistogram() *Histogram {
	return &Histogram{
		values: make([]int64, 0, 10000),
		minVal: int64(^uint64(0) >> 1), // max int64
		maxVal: 0,
	}
}

// Record records a latency measurement in nanoseconds.
func (h *Histogram) Record(latencyNs int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.values = append(h.values, latencyNs)
	h.count++
	h.sum += latencyNs
	h.sorted = false

	if latencyNs < h.minVal {
		h.minVal = latencyNs
	}
	if latencyNs > h.maxVal {
		h.maxVal = latencyNs
	}
}

// RecordDuration records a duration as a latency measurement.
func (h *Histogram) RecordDuration(d time.Duration) {
	h.Record(d.Nanoseconds())
}

// Count returns the number of recorded latencies.
func (h *Histogram) Count() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// ensureSorted sorts the values if not already sorted.
func (h *Histogram) ensureSorted() {
	if !h.sorted && len(h.values) > 0 {
		sort.Slice(h.values, func(i, j int) bool { return h.values[i] < h.values[j] })
		h.sorted = true
	}
}

// percentile computes the given percentile (0-100) without locking.
func (h *Histogram) percentile(p float64) int64 {
	if len(h.values) == 0 {
		return 0
	}
	if p <= 0 {
		return h.values[0]
	}
	if p >= 100 {
		return h.values[len(h.values)-1]
	}

	h.ensureSorted()
	idx := int64(float64(len(h.values)-1) * p / 100.0)
	if idx < 0 {
		idx = 0
	}
	if idx >= int64(len(h.values)) {
		idx = int64(len(h.values) - 1)
	}
	return h.values[idx]
}

// P50 returns the 50th percentile (median).
func (h *Histogram) P50() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Duration(h.percentile(50))
}

// P95 returns the 95th percentile.
func (h *Histogram) P95() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Duration(h.percentile(95))
}

// P99 returns the 99th percentile.
func (h *Histogram) P99() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Duration(h.percentile(99))
}

// P999 returns the 99.9th percentile.
func (h *Histogram) P999() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Duration(h.percentile(99.9))
}

// Min returns the minimum latency.
func (h *Histogram) Min() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.minVal == (int64(^uint64(0) >> 1)) {
		return 0
	}
	return time.Duration(h.minVal)
}

// Max returns the maximum latency.
func (h *Histogram) Max() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Duration(h.maxVal)
}

// Mean returns the mean latency.
func (h *Histogram) Mean() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0
	}
	return time.Duration(h.sum / h.count)
}

// Report returns a formatted latency report.
func (h *Histogram) Report() string {
	h.mu.Lock()
	defer h.mu.Unlock()

	count := h.count
	if count == 0 {
		return "Histogram: no data"
	}

	min := h.minVal
	if min == (int64(^uint64(0) >> 1)) {
		min = 0
	}
	mean := h.sum / count

	h.ensureSorted()

	return fmt.Sprintf(
		"Latency (count=%d): min=%v p50=%v p95=%v p99=%v max=%v mean=%v",
		count,
		time.Duration(min),
		time.Duration(h.percentile(50)),
		time.Duration(h.percentile(95)),
		time.Duration(h.percentile(99)),
		time.Duration(h.maxVal),
		time.Duration(mean),
	)
}
