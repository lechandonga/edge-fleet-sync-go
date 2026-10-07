// Package fleet wires the cloud registry, edge devices and the in-process
// transport into one process. All timing is driven by an injectable clock;
// RunOnce advances the world one "tick" without any goroutine or wall-clock
// wait, which keeps tests fast and deterministic. A background runner using
// a real clock is provided for local demos.
package fleet

import (
	"sync"

	"github.com/lechandonga/edge-fleet-sync-go/clock"
	"github.com/lechandonga/edge-fleet-sync-go/cloud"
	"github.com/lechandonga/edge-fleet-sync-go/edge"
	"github.com/lechandonga/edge-fleet-sync-go/protocol"
	"github.com/lechandonga/edge-fleet-sync-go/transport"
)

type member struct {
	device *edge.Device
}

// Fleet is the process-local cloud/edge simulation.
type Fleet struct {
	mu       sync.Mutex
	Clock    clock.Clock
	Cloud    *cloud.Registry
	Trans    *transport.InProc
	hbEvery  int64 // ms between heartbeats per device
	lastBeat map[protocol.DeviceID]int64
	members  map[protocol.DeviceID]*member
}

func New(c clock.Clock, leaseTTLMs, heartbeatEveryMs int64) *Fleet {
	reg := cloud.NewRegistry(cloud.RegistryConfig{LeaseTTLMs: leaseTTLMs})
	tr := transport.New(reg)
	f := &Fleet{
		Clock: c, Cloud: reg, Trans: tr, hbEvery: heartbeatEveryMs,
		lastBeat: map[protocol.DeviceID]int64{},
		members:  map[protocol.DeviceID]*member{},
	}
	reg.SetLink(tr)
	return f
}

// Register creates both the cloud record and the edge agent, then connects.
func (f *Fleet) Register(id protocol.DeviceID, key string, firmwareVersion int64, bufferCapacity int) (*edge.Device, error) {
	if err := f.Cloud.Register(id, key, firmwareVersion); err != nil {
		if rj, ok := err.(*protocol.RejectError); !ok || rj.Code != protocol.RejectDuplicateRegistration {
			return nil, err
		}
	}
	f.Trans.Attach(id)
	dev := edge.New(id, key, firmwareVersion, bufferCapacity, transport.DeviceUplink{T: f.Trans, ID: id})
	lease, err := f.Cloud.Connect(id, key, f.Clock)
	if err != nil {
		return nil, err
	}
	dev.Session(lease)
	f.mu.Lock()
	f.members[id] = &member{device: dev}
	f.mu.Unlock()
	return dev, nil
}

// Reconnect performs a fresh connect (new epoch) and reattaches the device.
// It also drains the mailbox: pushes queued while offline are flushed by the
// cloud inside Connect and applied locally.
func (f *Fleet) Reconnect(id protocol.DeviceID) error {
	f.mu.Lock()
	m := f.members[id]
	f.mu.Unlock()
	if m == nil {
		return protocol.NewReject(protocol.RejectUnknownDevice, "device %s", id)
	}
	lease, err := f.Cloud.Connect(id, m.device.Key(), f.Clock)
	if err != nil {
		return err
	}
	m.device.Session(lease)
	f.deliverMailbox(m.device)
	return nil
}

// Disconnect explicitly closes the session on both sides.
func (f *Fleet) Disconnect(id protocol.DeviceID) error {
	f.mu.Lock()
	m := f.members[id]
	f.mu.Unlock()
	if m == nil {
		return protocol.NewReject(protocol.RejectUnknownDevice, "device %s", id)
	}
	if err := f.Cloud.Disconnect(id, m.device.Lease()); err != nil {
		return err
	}
	m.device.GoOffline()
	return nil
}

// MarkOffline models an observed local link loss without cloud handshake.
func (f *Fleet) MarkOffline(id protocol.DeviceID) {
	f.mu.Lock()
	m := f.members[id]
	f.mu.Unlock()
	if m != nil {
		m.device.GoOffline()
	}
}

// deliverMailbox applies every queued config push to the device and flushes acks.
func (f *Fleet) deliverMailbox(dev *edge.Device) {
	mb := f.Trans.Mailbox(dev.ID())
	for {
		select {
		case env := <-mb:
			if env.Kind == protocol.KindConfigPush {
				_ = dev.ApplyConfigPush(env)
			}
		default:
			_ = dev.FlushAcks()
			return
		}
	}
}

// TickResult summarises one RunOnce pass for logging/assertions.
type TickResult struct {
	AtMs         int64
	Expired      []protocol.DeviceID
	Heartbeats   []protocol.DeviceID
	TelemetryOut int
	AcksFlushed  int
}

// RunOnce performs one deterministic maintenance pass: expiry sweep,
// heartbeats for connected devices, mailbox delivery, telemetry/ack flush.
func (f *Fleet) RunOnce() TickResult {
	now := f.Clock.NowMs()
	res := TickResult{AtMs: now}
	res.Expired = f.Cloud.ExpireSweep(f.Clock)
	for _, id := range res.Expired {
		f.MarkOffline(id)
	}

	f.mu.Lock()
	ids := make([]protocol.DeviceID, 0, len(f.members))
	for id := range f.members {
		ids = append(ids, id)
	}
	f.mu.Unlock()

	for _, id := range ids {
		m := f.members[id]
		dev := m.device
		if !dev.Connected() {
			continue
		}
		// heartbeat due?
		last := f.lastBeat[id]
		if now-last >= f.hbEvery {
			hb, ok := dev.NextHeartbeat(now)
			if ok {
				if _, err := f.Cloud.Heartbeat(id, dev.Lease(), hb, f.Clock); err == nil {
					res.Heartbeats = append(res.Heartbeats, id)
					f.lastBeat[id] = now
				} else if rj, ok := err.(*protocol.RejectError); ok && rj.Code == protocol.RejectStaleLease {
					dev.GoOffline()
				}
			}
		}
		f.deliverMailbox(dev)
		if n, err := dev.FlushTelemetry(); err == nil {
			res.TelemetryOut += n
		}
		if dev.PendingAckCount() > 0 {
			before := dev.PendingAckCount()
			_ = dev.FlushAcks()
			res.AcksFlushed += before - dev.PendingAckCount()
		}
	}
	return res
}

// Device returns the registered edge agent.
func (f *Fleet) Device(id protocol.DeviceID) *edge.Device {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m, ok := f.members[id]; ok {
		return m.device
	}
	return nil
}
