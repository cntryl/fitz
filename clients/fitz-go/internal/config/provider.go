package config

import (
	"sync"
)

// Provider is a singleton for global configuration.
type Provider struct {
	mu  sync.RWMutex
	cfg Config
}

var provider = &Provider{
	cfg: LoadFromEnv(DefaultConfig()),
}

// Get returns the current configuration.
func Get() Config {
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	return provider.cfg
}

// Set updates configuration (thread-safe).
func Set(cfg Config) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.cfg = cfg
}

// BufferCfg returns the buffer pool configuration.
func BufferCfg() BufferConfig {
	return Get().BufferConfig
}

// BatchCfg returns the batch configuration.
func BatchCfg() BatchConfig {
	return Get().BatchConfig
}

// ProfilingCfg returns the profiling configuration.
func ProfilingCfg() ProfilingConfig {
	return Get().ProfilingConfig
}
