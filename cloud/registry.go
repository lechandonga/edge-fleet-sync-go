package cloud

import (
	"sync"

	"github.com/lechandonga/edge-fleet-sync-go/protocol"
)

// deviceState is the cloud-side per-device record. All fields are guarded by
// Registry.mu.
type deviceState struct {
	id              protocol.DeviceID
	key             string
	firmwareVersion int64

	// lease / liveness
	epoch           int64 // bumped on every accepted (re)connect
	online          bool
	expiresAt       int64
	lastBeatCounter int64

	// config
	desiredVersion int64 // highest version accepted by the cloud
	appliedVersion int64 // highest version acked by the device

	// queued pushes while offline (FIFO), each appears once
	pending []*queuedPush
	// pushSeq -> push record, for idempotent acks/redelivery
	pushed map[string]*queuedPush

	// telemetry
	telemetry *telemetryState
}

type queuedPush struct {
	seq       int64
	envelope  *protocol.Envelope
	delivered bool // handed to a live link at least once
	acked     bool
}

// Registry owns the authoritative fleet state. It is safe for concurrent use.
type Registry struct {
	mu          sync.Mutex
	devices     map[protocol.DeviceID]*deviceState
	leaseTTLMs  int64
	nextPushSeq int64
	telemetry   *TelemetryStore

	// link is used to deliver envelopes to currently connected devices.
	link Link
}

type RegistryConfig struct {
	LeaseTTLMs int64
	Link       Link
}

func NewRegistry(cfg RegistryConfig) *Registry {
	if cfg.LeaseTTLMs <= 0 {
		cfg.LeaseTTLMs = 5000
	}
	return &Registry{
		devices:    map[protocol.DeviceID]*deviceState{},
		leaseTTLMs: cfg.LeaseTTLMs,
		link:       cfg.Link,
		telemetry:  NewTelemetryStore(),
	}
}

// TelemetryStore exposes the persisted telemetry view.
func (r *Registry) TelemetryStore() *TelemetryStore { return r.telemetry }

// Register adds a device. Re-registering the same id with the same key is an
// idempotent no-op; a different key is an identity conflict. Registration
// alone never marks the device online — Connect does that.
func (r *Registry) Register(id protocol.DeviceID, key string, firmwareVersion int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.devices[id]; ok {
		if d.key != key {
			return protocol.NewReject(protocol.RejectIdentityConflict,
				"device %s already registered with a different key", id)
		}
		// identical re-registration: idempotent, refresh firmware if newer
		if firmwareVersion > d.firmwareVersion {
			d.firmwareVersion = firmwareVersion
		}
		return protocol.NewReject(protocol.RejectDuplicateRegistration,
			"device %s is already registered (same key; no-op)", id)
	}
	r.devices[id] = &deviceState{
		id: id, key: key, firmwareVersion: firmwareVersion,
		pushed:    map[string]*queuedPush{},
		telemetry: newTelemetryState(),
	}
	return nil
}

// IsRegistered reports whether the device exists.
func (r *Registry) IsRegistered(id protocol.DeviceID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.devices[id]
	return ok
}

// SetLink wires the outbound transport. Intended for composition roots where
// the transport needs the registry and vice versa.
func (r *Registry) SetLink(l Link) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.link = l
}

// snapshotDevice copies the liveness/config fields callers may inspect.
type Snapshot struct {
	ID              protocol.DeviceID
	Online          bool
	Epoch           int64
	ExpiresAt       int64
	FirmwareVersion int64
	DesiredVersion  int64
	AppliedVersion  int64
	PendingCount    int64
}

func (r *Registry) Snapshot(id protocol.DeviceID) (Snapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[id]
	if !ok {
		return Snapshot{}, false
	}
	return Snapshot{
		ID: d.id, Online: d.online, Epoch: d.epoch, ExpiresAt: d.expiresAt,
		FirmwareVersion: d.firmwareVersion, DesiredVersion: d.desiredVersion,
		AppliedVersion: d.appliedVersion, PendingCount: int64(len(d.pending)),
	}, true
}
