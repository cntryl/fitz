package config

import (
	"os"
	"strconv"
	"time"
)

// Config is the global fitz-go client configuration.
type Config struct {
	// Connection configuration
	ConnConfig ConnConfig

	// Buffer pool configuration
	BufferConfig BufferConfig

	// Batching configuration
	BatchConfig BatchConfig

	// Profiling configuration
	ProfilingConfig ProfilingConfig
}

// ConnConfig manages connection-level settings.
type ConnConfig struct {
	AuthTimeout      time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	MaxFrameSize     int
	ReconnectEnabled bool
	ReconnectBackoff time.Duration
}

// BufferConfig manages buffer pool settings.
type BufferConfig struct {
	InitialPoolSize int  // FITZ_POOL_SIZE (default: 1024)
	MaxPoolSize     int  // FITZ_POOL_MAX (default: 10000)
	MaxBufferSize   int  // FITZ_POOL_MAX_BUFFER (default: 64KB)
	EnableMetrics   bool // FITZ_POOL_METRICS (default: true)
}

// BatchConfig manages request batching.
type BatchConfig struct {
	MaxBatchSize       int           // FITZ_BATCH_SIZE (default: 100)
	MaxBatchTimeout    time.Duration // FITZ_BATCH_TIMEOUT (default: 10ms)
	EnableBackpressure bool          // FITZ_BACKPRESSURE (default: true)
	BackpressureLimit  int           // FITZ_BACKPRESSURE_LIMIT (default: 10000)
}

// ProfilingConfig manages profiling and metrics collection.
type ProfilingConfig struct {
	EnableCPUProfile bool   // FITZ_CPU_PROFILE (default: false)
	EnableMemProfile bool   // FITZ_MEM_PROFILE (default: false)
	EnableDataRace   bool   // FITZ_DETECT_RACES (default: false)
	ProfileOutputDir string // FITZ_PROFILE_DIR (default: "./profiles")
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		ConnConfig: ConnConfig{
			AuthTimeout:      5 * time.Second,
			ReadTimeout:      30 * time.Second,
			WriteTimeout:     10 * time.Second,
			MaxFrameSize:     16 * 1024 * 1024,
			ReconnectEnabled: true,
			ReconnectBackoff: 1 * time.Second,
		},
		BufferConfig: BufferConfig{
			InitialPoolSize: 1024,
			MaxPoolSize:     10000,
			MaxBufferSize:   64 * 1024,
			EnableMetrics:   true,
		},
		BatchConfig: BatchConfig{
			MaxBatchSize:       100,
			MaxBatchTimeout:    10 * time.Millisecond,
			EnableBackpressure: true,
			BackpressureLimit:  10000,
		},
		ProfilingConfig: ProfilingConfig{
			EnableCPUProfile: false,
			EnableMemProfile: false,
			EnableDataRace:   false,
			ProfileOutputDir: "./profiles",
		},
	}
}

// LoadFromEnv applies environment variable overrides to configuration.
func LoadFromEnv(cfg Config) Config {
	if v := os.Getenv("FITZ_POOL_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.BufferConfig.InitialPoolSize = n
		}
	}
	if v := os.Getenv("FITZ_POOL_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.BufferConfig.MaxPoolSize = n
		}
	}
	if v := os.Getenv("FITZ_POOL_MAX_BUFFER"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.BufferConfig.MaxBufferSize = n
		}
	}
	if v := os.Getenv("FITZ_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.BatchConfig.MaxBatchSize = n
		}
	}
	if v := os.Getenv("FITZ_BATCH_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.BatchConfig.MaxBatchTimeout = d
		}
	}
	if v := os.Getenv("FITZ_BACKPRESSURE"); v == "false" {
		cfg.BatchConfig.EnableBackpressure = false
	}
	if v := os.Getenv("FITZ_BACKPRESSURE_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.BatchConfig.BackpressureLimit = n
		}
	}
	if v := os.Getenv("FITZ_CPU_PROFILE"); v == "true" {
		cfg.ProfilingConfig.EnableCPUProfile = true
	}
	if v := os.Getenv("FITZ_MEM_PROFILE"); v == "true" {
		cfg.ProfilingConfig.EnableMemProfile = true
	}
	if v := os.Getenv("FITZ_DETECT_RACES"); v == "true" {
		cfg.ProfilingConfig.EnableDataRace = true
	}
	if v := os.Getenv("FITZ_PROFILE_DIR"); v != "" {
		cfg.ProfilingConfig.ProfileOutputDir = v
	}
	return cfg
}
