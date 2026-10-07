package fleet

import (
	"sync"
	"sync/atomic"
)

// TelemetryStore is an in-memory, concurrency-safe telemetry sink with
// dedup by (device, seq). Gaps and out-of-order records are stored but
// flagged, never silently dropped. Availability can be toggled to simulate
// a temporarily unavailable backend.
type TelemetryStore struct {
	mu        sync.Mutex
	available atomic.Bool
	seen      map[DeviceID]map[uint64]struct{}
	maxSeq    map[DeviceID]uint64
	hasAny    map[DeviceID]bool
	records   []TelemetryRecord
	dupCount  int
}

func NewTelemetryStore() *TelemetryStore {
	s := &TelemetryStore{
		seen:   make(map[DeviceID]map[uint64]struct{}),
		maxSeq: make(map[DeviceID]uint64),
		hasAny: make(map[DeviceID]bool),
	}
	s.available.Store(true)
	return s
}

// SetAvailable simulates backend availability flapping.
func (s *TelemetryStore) SetAvailable(up bool) { s.available.Store(up) }

// Ingest stores one record. It returns stored=false for duplicates (an
// idempotent no-op, not an error) and ErrStoreUnavailable when the backend
// is down, in which case the caller must retain the record.
func (s *TelemetryStore) Ingest(id DeviceID, seq uint64, payload string) (bool, error) {
	if !s.available.Load() {
		return false, ErrStoreUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[id] == nil {
		s.seen[id] = make(map[uint64]struct{})
	}
	if _, dup := s.seen[id][seq]; dup {
		s.dupCount++
		return false, nil
	}
	s.seen[id][seq] = struct{}{}
	rec := TelemetryRecord{DeviceID: id, Seq: seq, Payload: payload}
	if s.hasAny[id] {
		if seq > s.maxSeq[id]+1 {
			rec.Flags = append(rec.Flags, FlagGap)
		} else if seq < s.maxSeq[id] {
			rec.Flags = append(rec.Flags, FlagOutOfOrder)
		}
	} else if seq > 1 {
		rec.Flags = append(rec.Flags, FlagGap)
	}
	if !s.hasAny[id] || seq > s.maxSeq[id] {
		s.maxSeq[id] = seq
	}
	s.hasAny[id] = true
	s.records = append(s.records, rec)
	return true, nil
}

// Records returns all stored records in ingestion order.
func (s *TelemetryStore) Records() []TelemetryRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TelemetryRecord, len(s.records))
	copy(out, s.records)
	return out
}

// Duplicates returns how many duplicate deliveries were deduplicated.
func (s *TelemetryStore) Duplicates() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dupCount
}
