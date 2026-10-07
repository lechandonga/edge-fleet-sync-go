package fleet

import (
	"fmt"
	"sync"
	"time"
)

type deviceState struct {
	identity  string
	lease     Lease
	online    bool
	lastHbSeq uint64
	hasHb     bool
}

// Registry manages device registration, heartbeats and leases.
// It is safe for concurrent use.
type Registry struct {
	mu      sync.Mutex
	clock   Clock
	ttl     time.Duration
	nextID  uint64
	devices map[DeviceID]*deviceState
}

func NewRegistry(clock Clock, leaseTTL time.Duration) *Registry {
	return &Registry{clock: clock, ttl: leaseTTL, devices: make(map[DeviceID]*deviceState)}
}

// Register registers a device with an identity token. Re-registering an
// active device is rejected with ErrAlreadyRegistered (same identity) or
// ErrIdentityConflict (different identity). A device whose lease expired may
// register again and receives a fresh lease.
func (r *Registry) Register(id DeviceID, identity string) (Lease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock.Now()
	if st, ok := r.devices[id]; ok && r.aliveLocked(st, now) {
		if st.identity != identity {
			return Lease{}, reject(id, ErrIdentityConflict, "device id held by another identity")
		}
		return Lease{}, reject(id, ErrAlreadyRegistered, fmt.Sprintf("lease %d still valid", st.lease.ID))
	}
	r.nextID++
	lease := Lease{ID: r.nextID, IssuedAt: now, ExpiresAt: now.Add(r.ttl)}
	r.devices[id] = &deviceState{identity: identity, lease: lease, online: true}
	return lease, nil
}

// Heartbeat refreshes a device's lease. hbSeq must strictly increase per
// device; an out-of-order heartbeat is rejected but does not take the device
// offline. An expired lease marks the device offline.
func (r *Registry) Heartbeat(id DeviceID, leaseID uint64, hbSeq uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock.Now()
	st, ok := r.devices[id]
	if !ok {
		return reject(id, ErrUnknownDevice, "not registered")
	}
	if st.lease.ID != leaseID {
		return reject(id, ErrStaleLease, fmt.Sprintf("have lease %d, current is %d", leaseID, st.lease.ID))
	}
	if !r.aliveLocked(st, now) {
		st.online = false
		return reject(id, ErrLeaseExpired, fmt.Sprintf("lease %d expired", leaseID))
	}
	if st.hasHb && hbSeq <= st.lastHbSeq {
		return reject(id, ErrHeartbeatOutOfOrder, fmt.Sprintf("seq %d <= last %d", hbSeq, st.lastHbSeq))
	}
	st.hasHb = true
	st.lastHbSeq = hbSeq
	st.online = true
	st.lease.ExpiresAt = now.Add(r.ttl)
	return nil
}

// Online reports whether the device currently holds a valid lease.
func (r *Registry) Online(id DeviceID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.devices[id]
	return ok && st.online && r.aliveLocked(st, r.clock.Now())
}

// CurrentLease returns the active lease ID for a device, 0 if unknown.
func (r *Registry) CurrentLease(id DeviceID) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.devices[id]; ok {
		return st.lease.ID
	}
	return 0
}

func (r *Registry) aliveLocked(st *deviceState, now time.Time) bool {
	return now.Before(st.lease.ExpiresAt)
}
