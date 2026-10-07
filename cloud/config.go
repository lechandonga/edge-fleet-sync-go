package cloud

import (
	"fmt"

	"github.com/lechandonga/edge-fleet-sync-go/protocol"
)

// Link delivers cloud->device envelopes. The in-process transport implements
// it; tests can inject a fake to simulate a dropped link.
type Link interface {
	SendToDevice(env *protocol.Envelope)
}

// PushConfig accepts a versioned config for a device.
//
// Rules:
//   - device must be registered;
//   - version must be strictly greater than the desired version (rollback is
//     refused; an identical re-push of the in-flight version is idempotent and
//     returns the existing envelope so a lost delivery can be retried);
//   - the config firmware window must contain the device firmware.
//
// If the device is offline the push is queued FIFO and flushed on reconnect.
func (r *Registry) PushConfig(id protocol.DeviceID, cfg protocol.Config) (*protocol.Envelope, error) {
	r.mu.Lock()
	d, ok := r.devices[id]
	if !ok {
		r.mu.Unlock()
		return nil, protocol.NewReject(protocol.RejectUnknownDevice, "device %s", id)
	}
	if cfg.Version < d.desiredVersion || (cfg.Version == d.desiredVersion && d.desiredVersion != 0) {
		// Idempotent re-push of the exact current desired version: return the
		// already-queued/delivered envelope for redelivery.
		if cfg.Version == d.desiredVersion {
			if q := r.findPushLocked(d, cfg.Version); q != nil {
				env := q.envelope
				online := d.online
				r.mu.Unlock()
				if online {
					r.deliver(env) // ack lost -> redeliver same DedupKey
				}
				return env, nil
			}
		}
		// Exact re-push of an already-effective version (e.g. a retry after
		// ack cleanup) is an idempotent no-op; strictly lower versions remain
		// rollbacks and are rejected below.
		if cfg.Version == d.appliedVersion && cfg.Version > 0 {
			r.mu.Unlock()
			return &protocol.Envelope{
				ID:       fmt.Sprintf("cfg-%s-%d", id, cfg.Version),
				DeviceID: id,
				Kind:     protocol.KindConfigPush,
				DedupKey: fmt.Sprintf("config:%d", cfg.Version),
				Payload:  protocol.ConfigPush{Config: cfg, FirmwareVersion: d.firmwareVersion},
			}, nil
		}
		r.mu.Unlock()
		return nil, protocol.NewReject(protocol.RejectConfigRollback,
			"config v%d does not advance desired v%d for %s",
			cfg.Version, d.desiredVersion, id)
	}
	if cfg.FirmwareMax != 0 && (d.firmwareVersion < cfg.FirmwareMin || d.firmwareVersion > cfg.FirmwareMax) {
		r.mu.Unlock()
		return nil, protocol.NewReject(protocol.RejectConfigIncompatible,
			"device %s firmware v%d outside config v%d window [%d,%d]",
			id, d.firmwareVersion, cfg.Version, cfg.FirmwareMin, cfg.FirmwareMax)
	}

	r.nextPushSeq++
	env := &protocol.Envelope{
		ID:       fmt.Sprintf("cfg-%s-%d", id, cfg.Version),
		DeviceID: id,
		Kind:     protocol.KindConfigPush,
		DedupKey: fmt.Sprintf("config:%d", cfg.Version),
		Payload: protocol.ConfigPush{
			Config: cfg, FirmwareVersion: d.firmwareVersion,
		},
	}
	q := &queuedPush{seq: r.nextPushSeq, envelope: env}
	d.pending = append(d.pending, q)
	d.pushed[env.DedupKey] = q
	d.desiredVersion = cfg.Version
	online := d.online
	if online {
		q.delivered = true
	}
	r.mu.Unlock()

	if online {
		r.deliver(env)
	}
	return env, nil
}

// AckConfig records that a config version is effective on the device.
// Acks are idempotent: re-acking the same version (e.g. the ack retry arrived
// after the cloud already processed it) is a success no-op.
func (r *Registry) AckConfig(id protocol.DeviceID, ack protocol.ConfigAck) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[id]
	if !ok {
		return protocol.NewReject(protocol.RejectUnknownDevice, "device %s", id)
	}
	if ack.Version > 0 && ack.Version <= d.appliedVersion {
		// Duplicate/reordered ack for an already-effective version.
		return nil
	}
	q := r.findPushLocked(d, ack.Version)
	if q == nil {
		return protocol.NewReject(protocol.RejectConfigStaleAck,
			"version %d was never issued to %s", ack.Version, id)
	}
	q.acked = true
	d.appliedVersion = ack.Version
	// Drop every queued push up to and including this version.
	kept := d.pending[:0]
	for _, p := range d.pending {
		push := p.envelope.Payload.(protocol.ConfigPush)
		if push.Config.Version <= ack.Version {
			delete(d.pushed, p.envelope.DedupKey)
			continue
		}
		kept = append(kept, p)
	}
	d.pending = kept
	return nil
}

func (r *Registry) findPushLocked(d *deviceState, version int64) *queuedPush {
	return d.pushed[fmt.Sprintf("config:%d", version)]
}

// PendingConfigs returns the queued (not yet acked) versions for a device.
func (r *Registry) PendingConfigs(id protocol.DeviceID) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[id]
	if !ok {
		return nil
	}
	out := make([]int64, 0, len(d.pending))
	for _, q := range d.pending {
		out = append(out, q.envelope.Payload.(protocol.ConfigPush).Config.Version)
	}
	return out
}
