package fleet

import (
	"fmt"
	"sync"
)

// ConfigManager owns desired config versions, per-device pending queues and
// applied-version bookkeeping. Delivery is pull-based: an online device calls
// NextConfig, applies it, then Ack. Acks are idempotent; a lost ack simply
// causes a redelivery of the same version, which the device applies once.
type ConfigManager struct {
	mu       sync.Mutex
	reg      *Registry
	desired  map[DeviceID]uint64
	applied  map[DeviceID]uint64
	queue    map[DeviceID][]Config
	inflight map[DeviceID]*Config
}

func NewConfigManager(reg *Registry) *ConfigManager {
	return &ConfigManager{
		reg:      reg,
		desired:  make(map[DeviceID]uint64),
		applied:  make(map[DeviceID]uint64),
		queue:    make(map[DeviceID][]Config),
		inflight: make(map[DeviceID]*Config),
	}
}

// Push stages a config for a device. Versions must move strictly forward and
// the config must be compatible with the device's applied version. If the
// device is offline the config is queued and delivered in order on recovery.
func (m *ConfigManager) Push(id DeviceID, cfg Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	applied := m.applied[id]
	if cfg.Version <= applied {
		return reject(id, ErrVersionRegression,
			fmt.Sprintf("version %d <= applied %d", cfg.Version, applied))
	}
	if cfg.Version <= m.desired[id] {
		return reject(id, ErrVersionRegression,
			fmt.Sprintf("version %d <= desired %d", cfg.Version, m.desired[id]))
	}
	if cfg.RequiresVersion > applied {
		return reject(id, ErrIncompatibleConfig,
			fmt.Sprintf("requires applied >= %d, have %d", cfg.RequiresVersion, applied))
	}
	m.desired[id] = cfg.Version
	m.queue[id] = append(m.queue[id], cfg)
	return nil
}

// NextConfig returns the oldest undelivered config for an online device.
// Offline devices receive nothing: tasks are never pushed to them.
func (m *ConfigManager) NextConfig(id DeviceID) (Config, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.reg.Online(id) {
		return Config{}, false, nil
	}
	if cfg, ok := m.inflight[id]; ok {
		return *cfg, true, nil // redeliver until acked
	}
	q := m.queue[id]
	if len(q) == 0 {
		return Config{}, false, nil
	}
	cfg := q[0]
	m.inflight[id] = &cfg
	return cfg, true, nil
}

// Ack confirms application of a version. Duplicate or stale acks are
// idempotently ignored so redelivery never causes divergence.
func (m *ConfigManager) Ack(id DeviceID, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cfg, ok := m.inflight[id]
	if !ok || cfg.Version != version {
		return
	}
	delete(m.inflight, id)
	m.queue[id] = m.queue[id][1:]
	if version > m.applied[id] {
		m.applied[id] = version
	}
}

// AppliedVersion returns the newest acked version for a device.
func (m *ConfigManager) AppliedVersion(id DeviceID) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applied[id]
}

// Pending returns the number of queued plus in-flight configs.
func (m *ConfigManager) Pending(id DeviceID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.queue[id])
	if _, ok := m.inflight[id]; ok {
		n++
	}
	return n
}
