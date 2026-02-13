package benchkit

import (
	"encoding/json"
	"io/ioutil"
	"time"
)

// BaselineResult captures a single benchmark result for comparison.
type BaselineResult struct {
	Name        string        `json:"name"`
	OpCount     int64         `json:"op_count"`
	TotalTime   time.Duration `json:"total_time"`
	AllocBytes  uint64        `json:"alloc_bytes"`
	AllocCount  uint64        `json:"alloc_count"`
	NSPerOp     float64       `json:"ns_per_op"`
	BytesPerOp  float64       `json:"bytes_per_op"`
	AllocsPerOp float64       `json:"allocs_per_op"`
	Timestamp   time.Time     `json:"timestamp"`
}

// Baseline is a collection of benchmark results.
type Baseline struct {
	Version   string           `json:"version"`
	Timestamp time.Time        `json:"timestamp"`
	Results   []BaselineResult `json:"results"`
}

// SaveBaseline persists results to file for comparison.
func SaveBaseline(filename string, results []BaselineResult) error {
	b := Baseline{
		Version:   "1.0",
		Timestamp: time.Now(),
		Results:   results,
	}
	data, _ := json.MarshalIndent(b, "", "  ")
	return ioutil.WriteFile(filename, data, 0644)
}

// LoadBaseline loads a baseline from file.
func LoadBaseline(filename string) (map[string]BaselineResult, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var b Baseline
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, err
	}
	m := make(map[string]BaselineResult)
	for _, r := range b.Results {
		m[r.Name] = r
	}
	return m, nil
}

// CompareBaseline compares current results to baseline and returns percent change.
// Positive values indicate regression (slower/more allocs), negative values indicate improvement.
func CompareBaseline(baseline, current BaselineResult) float64 {
	if baseline.NSPerOp == 0 {
		return 0
	}
	return (current.NSPerOp - baseline.NSPerOp) / baseline.NSPerOp * 100
}

// CompareAllocations compares allocation rates.
func CompareAllocations(baseline, current BaselineResult) float64 {
	if baseline.AllocsPerOp == 0 {
		return 0
	}
	return (current.AllocsPerOp - baseline.AllocsPerOp) / baseline.AllocsPerOp * 100
}
