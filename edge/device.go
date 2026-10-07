package edge

import (
	"errors"
	"sync"

	"github.com/lechandonga/edge-fleet-sync-go/protocol"
)

// ErrOffline is returned by flush attempts while the device has no live link.
var ErrOffline = errors.New("device offline: uplink unavailable")

// OverflowPolicy names the cache eviction strategy used when the bounded
// telemetry buffer is full.
type OverflowPolicy string

const (
	// OverflowDropOldest evicts the lowest-seq buffered records so that the
	// newest observations survive. Evicted seqs are recorded in DroppedSeqs;
	// the cloud will flag exactly those seqs as missing after reconnection.
	OverflowDropOldest OverflowPolicy = "drop_oldest"
)

// Uplink is the transport capability a Device needs from its environment.
type Uplink interface {
	// SendTelemetry delivers a batch. Returning an error means "backend
	// unavailable": the records stay buffered and are retried later.
	SendTelemetry(batch protocol.TelemetryBatch) (protocol.TelemetryStatus, error)
	// SendConfigAck delivers an ack; errors trigger a later retry.
	SendConfigAck(id protocol.DeviceID, ack protocol.ConfigAck) error
}

// Device is the edge-side agent state. It is safe for concurrent use so
// multiple producer goroutines can Record while a flusher drains.
type Device struct {
	mu sync.Mutex

	id              protocol.DeviceID
	key             string
	firmwareVersion int64

	lease       protocol.Lease
	connected   bool
	beatCounter int64

	// config state
	appliedVersion int64
	appliedBody    string
	ackedVersions  map[int64]struct{}
	pendingAcks    []int64

	// telemetry bounded buffer
	capacity    int
	policy      OverflowPolicy
	nextSeq     int64
	buffer      []protocol.TelemetryRecord // ascending seq
	droppedSeqs []int64                    // audit trail of evictions
	seenPush    map[string]struct{}
	applyCount  int

	uplink Uplink
}

func New(id protocol.DeviceID, key string, firmwareVersion int64, capacity int, uplink Uplink) *Device {
	return &Device{
		id: id, key: key, firmwareVersion: firmwareVersion,
		capacity: capacity, policy: OverflowDropOldest, nextSeq: 1,
		ackedVersions: map[int64]struct{}{},
		seenPush:      map[string]struct{}{},
		uplink:        uplink,
	}
}

func (d *Device) ID() protocol.DeviceID  { return d.id }
func (d *Device) Key() string            { return d.key }
func (d *Device) FirmwareVersion() int64 { return d.firmwareVersion }
func (d *Device) Lease() protocol.Lease {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lease
}

func (d *Device) Connected() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.connected
}

func (d *Device) AppliedVersion() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.appliedVersion
}

// Session attaches a freshly minted cloud lease to the device.
func (d *Device) Session(lease protocol.Lease) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lease = lease
	d.connected = true
	d.beatCounter = 0
}

// GoOffline models a link loss or cloud-side expiry observed locally.
// Buffered telemetry and pending acks are retained.
func (d *Device) GoOffline() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.connected = false
}

// NextHeartbeat returns the next heartbeat envelope for the current session.
func (d *Device) NextHeartbeat(nowMs int64) (protocol.Heartbeat, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.connected {
		return protocol.Heartbeat{}, false
	}
	d.beatCounter++
	return protocol.Heartbeat{Epoch: d.lease.Epoch, Counter: d.beatCounter, NowMs: nowMs}, true
}

// Record appends one observation. It never blocks and never returns an error:
// if the bounded buffer overflows the configured policy evicts records and
// the evicted seqs are reported for audit.
func (d *Device) Record(atMs int64, metric string, value float64) (seq int64, evicted []int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	seq = d.nextSeq
	d.nextSeq++
	rec := protocol.TelemetryRecord{DeviceID: d.id, Seq: seq, AtMs: atMs, Metric: metric, Value: value}
	if len(d.buffer) >= d.capacity {
		need := len(d.buffer) + 1 - d.capacity
		evicted = make([]int64, need)
		for i := 0; i < need; i++ {
			evicted[i] = d.buffer[i].Seq
		}
		d.droppedSeqs = append(d.droppedSeqs, evicted...)
		d.buffer = d.buffer[need:]
	}
	d.buffer = append(d.buffer, rec)
	return seq, evicted
}

// BufferedCount returns how many records are waiting for upload.
func (d *Device) BufferedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.buffer)
}

// DroppedSeqs returns the audit trail of overflow-evicted seqs.
func (d *Device) DroppedSeqs() []int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]int64, len(d.droppedSeqs))
	copy(out, d.droppedSeqs)
	return out
}

// FlushTelemetry drains buffered records in seq order. On transport error the
// unsent records remain buffered (the batch is kept intact, so retries remain
// gap-free). Duplicate delivery after an ambiguous result is safe: the cloud
// deduplicates by seq.
func (d *Device) FlushTelemetry() (sent int, err error) {
	d.mu.Lock()
	if len(d.buffer) == 0 || d.uplink == nil {
		d.mu.Unlock()
		return 0, nil
	}
	if !d.connected {
		d.mu.Unlock()
		return 0, ErrOffline
	}
	batch := make([]protocol.TelemetryRecord, len(d.buffer))
	copy(batch, d.buffer)
	uplink := d.uplink
	d.mu.Unlock()

	status, err := uplink.SendTelemetry(protocol.TelemetryBatch{Records: batch})
	if err != nil {
		return 0, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	// Only drop seqs the cloud holds: accepted plus duplicates (already there).
	confirmed := make(map[int64]struct{}, len(status.AcceptedSeqs)+len(status.DupSeqs))
	for _, s := range status.AcceptedSeqs {
		confirmed[s] = struct{}{}
	}
	for _, s := range status.DupSeqs {
		confirmed[s] = struct{}{}
	}
	kept := d.buffer[:0]
	for _, rec := range d.buffer {
		if _, ok := confirmed[rec.Seq]; ok {
			sent++
			continue
		}
		kept = append(kept, rec)
	}
	d.buffer = kept
	return sent, nil
}

// ApplyConfigPush handles a cloud config envelope. Redelivery of the same
// DedupKey (e.g. the original ack was lost) applies exactly once but always
// yields an ack, so cloud/edge cannot diverge.
func (d *Device) ApplyConfigPush(env *protocol.Envelope) error {
	push, ok := env.Payload.(protocol.ConfigPush)
	if !ok {
		return protocol.NewReject(protocol.RejectConfigIncompatible, "bad payload")
	}
	cfg := push.Config
	d.mu.Lock()
	defer d.mu.Unlock()

	if cfg.Version < d.appliedVersion {
		return protocol.NewReject(protocol.RejectConfigRollback,
			"edge refuses v%d, already at v%d", cfg.Version, d.appliedVersion)
	}
	if _, dup := d.seenPush[env.DedupKey]; !dup {
		// Validate firmware window locally too.
		if cfg.FirmwareMax != 0 && (d.firmwareVersion < cfg.FirmwareMin || d.firmwareVersion > cfg.FirmwareMax) {
			return protocol.NewReject(protocol.RejectConfigIncompatible,
				"edge firmware v%d outside [%d,%d]", d.firmwareVersion, cfg.FirmwareMin, cfg.FirmwareMax)
		}
		d.seenPush[env.DedupKey] = struct{}{}
		if cfg.Version > d.appliedVersion {
			d.appliedVersion = cfg.Version
			d.appliedBody = cfg.Body
			d.applyCount++
		}
	}
	// (Re)arm the ack; duplicates collapse in pendingAcks.
	if _, acked := d.ackedVersions[cfg.Version]; !acked {
		already := false
		for _, v := range d.pendingAcks {
			if v == cfg.Version {
				already = true
				break
			}
		}
		if !already {
			d.pendingAcks = append(d.pendingAcks, cfg.Version)
		}
	}
	return nil
}

// AppliedBody returns the currently effective config body.
func (d *Device) AppliedBody() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.appliedBody
}

// PendingAckCount reports config versions awaiting ack delivery.
func (d *Device) PendingAckCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pendingAcks)
}

// ApplyCountForTest reports how many distinct config versions took effect.
func (d *Device) ApplyCountForTest() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.applyCount
}

// FlushAcks delivers pending config acks in version order.
func (d *Device) FlushAcks() error {
	d.mu.Lock()
	if !d.connected || len(d.pendingAcks) == 0 || d.uplink == nil {
		d.mu.Unlock()
		return nil
	}
	pending := make([]int64, len(d.pendingAcks))
	copy(pending, d.pendingAcks)
	uplink := d.uplink
	epoch := d.lease.Epoch
	id := d.id
	d.mu.Unlock()

	for _, v := range pending {
		if err := uplink.SendConfigAck(id, protocol.ConfigAck{Version: v, Epoch: epoch}); err != nil {
			return err
		}
		d.mu.Lock()
		d.ackedVersions[v] = struct{}{}
		d.pendingAcks = removeInt64(d.pendingAcks, v)
		d.mu.Unlock()
	}
	return nil
}

func removeInt64(xs []int64, v int64) []int64 {
	out := xs[:0]
	for _, x := range xs {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}
