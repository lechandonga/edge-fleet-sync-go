package fleet

import (
	"errors"
	"sync"
)

// Link simulates the network path between a device and the controller.
// It can be up/down and can duplicate each outbound delivery to emulate
// at-least-once transports.
type Link struct {
	mu        sync.Mutex
	up        bool
	duplicate int // extra copies of each delivery
}

func NewLink() *Link { return &Link{up: true} }

func (l *Link) Up() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.up
}

func (l *Link) SetUp(up bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.up = up
}

// SetDuplicates makes every delivery happen 1+n times.
func (l *Link) SetDuplicates(n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.duplicate = n
}

func (l *Link) deliveries() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return 1 + l.duplicate
}

// TelemetryCache is a bounded, deterministic local buffer. On overflow it
// drops the oldest record (DropOldest policy) and counts the drop. Writes
// are purely local and never touch the backend, so they cannot block or
// fail due to backend availability.
type TelemetryCache struct {
	capacity int
	items    []TelemetryRecord
	dropped  uint64
}

func NewTelemetryCache(capacity int) *TelemetryCache {
	return &TelemetryCache{capacity: capacity}
}

// Add appends a record, evicting the oldest if full. Returns true if an
// eviction happened.
func (c *TelemetryCache) Add(rec TelemetryRecord) bool {
	evicted := false
	if c.capacity > 0 && len(c.items) >= c.capacity {
		c.items = c.items[1:]
		c.dropped++
		evicted = true
	}
	c.items = append(c.items, rec)
	return evicted
}

// Pop removes and returns the oldest record.
func (c *TelemetryCache) Pop() (TelemetryRecord, bool) {
	if len(c.items) == 0 {
		return TelemetryRecord{}, false
	}
	rec := c.items[0]
	c.items = c.items[1:]
	return rec, true
}

func (c *TelemetryCache) Len() int        { return len(c.items) }
func (c *TelemetryCache) Dropped() uint64 { return c.dropped }

// Device is an in-process edge agent. All operations are synchronous and
// deterministic; tests drive it step by step.
type Device struct {
	ID       DeviceID
	identity string

	reg   *Registry
	cfg   *ConfigManager
	store *TelemetryStore
	Link  *Link

	lease     Lease
	hasLease  bool
	hbSeq     uint64
	nextSeq   uint64
	Cache     *TelemetryCache
	Applied   Config
	HasConfig bool
	// ApplyCount counts real config applications; idempotent redeliveries
	// must not increase it.
	ApplyCount int
}

func NewDevice(id DeviceID, identity string, reg *Registry, cfg *ConfigManager, store *TelemetryStore, cacheCap int) *Device {
	return &Device{
		ID:       id,
		identity: identity,
		reg:      reg,
		cfg:      cfg,
		store:    store,
		Link:     NewLink(),
		Cache:    NewTelemetryCache(cacheCap),
	}
}

// Connect registers or re-attaches the device. A device holding a stale
// lease (e.g. after expiry) re-registers and gets a fresh lease; it never
// overwrites a newer valid lease held by the controller.
func (d *Device) Connect() error {
	if !d.Link.Up() {
		return errors.New("fleet: link down")
	}
	if d.hasLease {
		err := d.reg.Heartbeat(d.ID, d.lease.ID, d.nextHbSeq())
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrStaleLease) || errors.Is(err, ErrUnknownDevice) || errors.Is(err, ErrLeaseExpired) {
			d.hasLease = false
		} else {
			return err
		}
	}
	lease, err := d.reg.Register(d.ID, d.identity)
	if err != nil {
		return err
	}
	d.lease = lease
	d.hasLease = true
	return nil
}

// Heartbeat sends one heartbeat. The error, if any, carries a
// distinguishable rejection reason.
func (d *Device) Heartbeat() error {
	if !d.Link.Up() {
		return errors.New("fleet: link down")
	}
	if !d.hasLease {
		return reject(d.ID, ErrUnknownDevice, "no lease held")
	}
	return d.reg.Heartbeat(d.ID, d.lease.ID, d.nextHbSeq())
}

func (d *Device) nextHbSeq() uint64 {
	d.hbSeq++
	return d.hbSeq
}

// SyncConfig pulls and applies pending configs in version order. Duplicate
// deliveries of the already-applied version are acked again without being
// re-applied, so both sides converge.
func (d *Device) SyncConfig() error {
	if !d.Link.Up() {
		return errors.New("fleet: link down")
	}
	for {
		var cfg Config
		var ok bool
		var err error
		for i := 0; i < d.Link.deliveries(); i++ {
			cfg, ok, err = d.cfg.NextConfig(d.ID)
			if err != nil {
				return err
			}
			if ok {
				d.applyOnce(cfg)
			}
		}
		if !ok {
			return nil
		}
		d.cfg.Ack(d.ID, cfg.Version)
	}
}

func (d *Device) applyOnce(cfg Config) {
	if d.HasConfig && cfg.Version <= d.Applied.Version {
		return // duplicate delivery: ack but do not re-apply
	}
	d.Applied = cfg
	d.HasConfig = true
	d.ApplyCount++
}

// RecordTelemetry assigns the next sequence number and buffers the record
// locally. It never touches the backend, so it cannot block or fail when
// the store is down.
func (d *Device) RecordTelemetry(payload string) bool {
	d.nextSeq++
	return d.Cache.Add(TelemetryRecord{DeviceID: d.ID, Seq: d.nextSeq, Payload: payload})
}

// FlushTelemetry uploads buffered records in sequence order while the link
// is up and the backend is available. Unsent records stay buffered; the
// link may duplicate deliveries, which the store deduplicates.
func (d *Device) FlushTelemetry() (sent int, err error) {
	if !d.Link.Up() {
		return 0, errors.New("fleet: link down")
	}
	for {
		rec, ok := d.Cache.Pop()
		if !ok {
			return sent, nil
		}
		storedAny := false
		failed := false
		for i := 0; i < d.Link.deliveries(); i++ {
			_, ierr := d.store.Ingest(rec.DeviceID, rec.Seq, rec.Payload)
			if errors.Is(ierr, ErrStoreUnavailable) {
				failed = true
				break
			}
			if ierr != nil {
				return sent, ierr
			}
			storedAny = true
		}
		if failed {
			// backend down: put the record back at the head and stop
			d.Cache.items = append([]TelemetryRecord{rec}, d.Cache.items...)
			return sent, nil
		}
		if storedAny {
			sent++
		}
	}
}
