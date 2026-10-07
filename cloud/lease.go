package cloud

import (
	"github.com/lechandonga/edge-fleet-sync-go/clock"
	"github.com/lechandonga/edge-fleet-sync-go/protocol"
)

// Connect marks a registered device live and mints a new lease. Every prior
// epoch for the device is immediately invalidated. After a successful connect
// the cloud flushes queued config pushes in FIFO order.
func (r *Registry) Connect(id protocol.DeviceID, key string, now clock.Clock) (protocol.Lease, error) {
	r.mu.Lock()
	d, err := r.authDeviceLocked(id, key)
	if err != nil {
		r.mu.Unlock()
		return protocol.Lease{}, err
	}
	d.epoch++
	d.online = true
	d.lastBeatCounter = 0
	d.expiresAt = now.NowMs() + r.leaseTTLMs
	lease := protocol.Lease{DeviceID: id, Epoch: d.epoch, ExpiresAt: d.expiresAt}
	// Snapshot work to perform after unlocking (Link delivery must not be
	// invoked under the registry lock).
	type flush struct {
		envs []*protocol.Envelope
	}
	f := flush{}
	r.nextPushSeq++
	for _, q := range d.pending {
		q.delivered = true
		f.envs = append(f.envs, q.envelope)
	}
	r.mu.Unlock()

	for _, env := range f.envs {
		r.deliver(env)
	}
	return lease, nil
}

// Disconnect is an explicit goodbye. The device becomes offline but its
// registration and desired/applied versions are retained.
func (r *Registry) Disconnect(id protocol.DeviceID, lease protocol.Lease) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[id]
	if !ok {
		return protocol.NewReject(protocol.RejectUnknownDevice, "device %s", id)
	}
	if lease.Epoch < d.epoch {
		return protocol.NewReject(protocol.RejectStaleLease,
			"disconnect epoch %d older than current %d", lease.Epoch, d.epoch)
	}
	d.online = false
	d.expiresAt = 0
	return nil
}

// Heartbeat renews the lease. Rejected reasons are distinct:
// unknown device, stale lease (old epoch), out-of-order counter, and the
// implicit "already expired / superseded" case which is reported as stale.
func (r *Registry) Heartbeat(id protocol.DeviceID, lease protocol.Lease, hb protocol.Heartbeat, now clock.Clock) (protocol.Lease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[id]
	if !ok {
		return protocol.Lease{}, protocol.NewReject(protocol.RejectUnknownDevice, "device %s", id)
	}
	if lease.Epoch != d.epoch || lease.Epoch != hb.Epoch {
		return protocol.Lease{}, protocol.NewReject(protocol.RejectStaleLease,
			"heartbeat epoch %d (lease %d) != current epoch %d",
			hb.Epoch, lease.Epoch, d.epoch)
	}
	if !d.online {
		return protocol.Lease{}, protocol.NewReject(protocol.RejectStaleLease,
			"device %s epoch %d is not live (expired or disconnected)", id, lease.Epoch)
	}
	if hb.Counter <= d.lastBeatCounter {
		return protocol.Lease{}, protocol.NewReject(protocol.RejectHeartbeatOrder,
			"heartbeat counter %d <= last accepted %d", hb.Counter, d.lastBeatCounter)
	}
	d.lastBeatCounter = hb.Counter
	d.expiresAt = now.NowMs() + r.leaseTTLMs
	return protocol.Lease{DeviceID: id, Epoch: d.epoch, ExpiresAt: d.expiresAt}, nil
}

// ExpireSweep is driven by a clock tick (tests call it directly). Any live
// device whose lease expired is flipped offline; queued pushes are retained.
// It returns the ids that transitioned to offline.
func (r *Registry) ExpireSweep(now clock.Clock) []protocol.DeviceID {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := now.NowMs()
	var expired []protocol.DeviceID
	for _, d := range r.devices {
		if d.online && d.expiresAt != 0 && d.expiresAt <= t {
			d.online = false
			d.expiresAt = 0
			expired = append(expired, d.id)
		}
	}
	return expired
}

func (r *Registry) authDeviceLocked(id protocol.DeviceID, key string) (*deviceState, error) {
	d, ok := r.devices[id]
	if !ok {
		return nil, protocol.NewReject(protocol.RejectUnknownDevice, "device %s", id)
	}
	if d.key != key {
		return nil, protocol.NewReject(protocol.RejectIdentityConflict,
			"bad key for device %s", id)
	}
	return d, nil
}

func (r *Registry) deliver(env *protocol.Envelope) {
	if r.link != nil {
		r.link.SendToDevice(env)
	}
}
