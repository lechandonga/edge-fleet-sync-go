package transport

import (
	"sync"

	"github.com/lechandonga/edge-fleet-sync-go/cloud"
	"github.com/lechandonga/edge-fleet-sync-go/protocol"
)

// InProc is a fully in-memory cloud<->edge transport. It models a real link:
// deliveries for a disconnected device are dropped (the cloud queues config
// separately), uplinks can be switched down to simulate backend outages, and
// every delivery is recorded so tests can assert redelivery behaviour.
type InProc struct {
	mu sync.Mutex

	registry *cloud.Registry

	// device mailbox: connected devices receive cloud pushes here
	mailboxes map[protocol.DeviceID]chan *protocol.Envelope

	// linkDown[device]: when true, cloud->device sends are dropped on the wire
	linkDown map[protocol.DeviceID]bool
	// uplinkDown[device]: when true, device->cloud sends return errBackend
	uplinkDown map[protocol.DeviceID]bool

	// delivery audit
	cloudPushes      []protocol.DeviceID
	telemetryBatches int
	configAcks       []int64
}

func New(reg *cloud.Registry) *InProc {
	t := &InProc{
		registry:   reg,
		mailboxes:  map[protocol.DeviceID]chan *protocol.Envelope{},
		linkDown:   map[protocol.DeviceID]bool{},
		uplinkDown: map[protocol.DeviceID]bool{},
	}
	return t
}

// Attach creates a mailbox for a device.
func (t *InProc) Attach(id protocol.DeviceID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.mailboxes[id]; !ok {
		t.mailboxes[id] = make(chan *protocol.Envelope, 1024)
	}
}

// Mailbox returns the receive channel for a device.
func (t *InProc) Mailbox(id protocol.DeviceID) <-chan *protocol.Envelope {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mailboxes[id]
}

// SetLinkDown simulates cloud->device network partition.
func (t *InProc) SetLinkDown(id protocol.DeviceID, down bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.linkDown[id] = down
}

// SetUplinkDown simulates device->cloud backend outage.
func (t *InProc) SetUplinkDown(id protocol.DeviceID, down bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.uplinkDown[id] = down
}

// SendToDevice implements cloud.Link. A push while the link is down is
// dropped on the wire (the cloud keeps the push in its per-device queue, so
// a later reconnect flushes it again with the same DedupKey).
func (t *InProc) SendToDevice(env *protocol.Envelope) {
	t.mu.Lock()
	t.cloudPushes = append(t.cloudPushes, env.DeviceID)
	down := t.linkDown[env.DeviceID]
	mb := t.mailboxes[env.DeviceID]
	t.mu.Unlock()
	if down || mb == nil {
		return
	}
	select {
	case mb <- env:
	default: // mailbox full => treat as dropped wire; never block the cloud
	}
}

// CloudPushCount reports how many cloud->device send attempts occurred.
func (t *InProc) CloudPushCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.cloudPushes)
}

// TelemetryBatchCount reports observed telemetry uplinks.
func (t *InProc) TelemetryBatchCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.telemetryBatches
}

// ConfigAckCount reports observed config acks.
func (t *InProc) ConfigAckCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.configAcks)
}
