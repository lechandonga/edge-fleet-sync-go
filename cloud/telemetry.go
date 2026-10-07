package cloud

import (
	"sort"
	"sync"

	"github.com/lechandonga/edge-fleet-sync-go/protocol"
)

// telemetryState tracks one device's ingest watermarks.
type telemetryState struct {
	stored    map[int64]protocol.TelemetryRecord // seq -> record (all retained incl. marked)
	nextSeq   int64                              // smallest seq not yet contiguously stored
	markedGap map[int64]struct{}
}

func newTelemetryState() *telemetryState {
	return &telemetryState{stored: map[int64]protocol.TelemetryRecord{}, nextSeq: 1, markedGap: map[int64]struct{}{}}
}

// TelemetryStore is the durable (in-memory) ingest backend. Writes never fail
// on the cloud side: the store is a map and does not model backend outages;
// the edge cache guarantees a flaky link cannot block producers.
type TelemetryStore struct {
	mu sync.Mutex
}

func NewTelemetryStore() *TelemetryStore { return &TelemetryStore{} }

// Ingest applies one batch to a device's state. Semantics:
//   - duplicate seq: counted as dup, not stored twice;
//   - seq < nextSeq and unknown: out-of-order, stored and marked;
//   - seq > nextSeq: stored and marked; the hole range is reported missing;
//   - contiguous fill advances nextSeq and clears matching gap marks.
func (r *Registry) IngestTelemetry(batch protocol.TelemetryBatch) protocol.TelemetryStatus {
	if len(batch.Records) == 0 {
		return protocol.TelemetryStatus{}
	}
	id := batch.Records[0].DeviceID
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[id]
	if !ok {
		// Unknown device telemetry is rejected loudly rather than silently
		// dropped; callers see an empty status plus the error path.
		return protocol.TelemetryStatus{}
	}
	st := d.telemetry
	status := protocol.TelemetryStatus{}

	// Sort within batch so a batch carrying [3,1,2] still converges correctly
	// after a redelivery, while the originally-missing seqs stay marked.
	recs := make([]protocol.TelemetryRecord, len(batch.Records))
	copy(recs, batch.Records)
	sort.Slice(recs, func(i, j int) bool { return recs[i].Seq < recs[j].Seq })

	for _, rec := range recs {
		if _, dup := st.stored[rec.Seq]; dup {
			status.DupSeqs = append(status.DupSeqs, rec.Seq)
			continue
		}
		st.stored[rec.Seq] = rec
		status.AcceptedSeqs = append(status.AcceptedSeqs, rec.Seq)
		switch {
		case rec.Seq < st.nextSeq:
			// Stored below the contiguous watermark with that seq previously
			// unknown: it is retained and flagged out-of-order.
			status.OutOfOrder = append(status.OutOfOrder, rec.Seq)
			st.markedGap[rec.Seq] = struct{}{}
			status.Marked = append(status.Marked, rec.Seq)
		case rec.Seq > st.nextSeq:
			for s := st.nextSeq; s < rec.Seq; s++ {
				if _, have := st.stored[s]; !have {
					if _, flagged := st.markedGap[s]; !flagged {
						status.MissingSeqs = append(status.MissingSeqs, s)
					}
					st.markedGap[s] = struct{}{}
				}
			}
		}
		// advance contiguous watermark
		for {
			if _, have := st.stored[st.nextSeq]; !have {
				break
			}
			delete(st.markedGap, st.nextSeq)
			st.nextSeq++
		}
	}
	sort.Slice(status.MissingSeqs, func(i, j int) bool { return status.MissingSeqs[i] < status.MissingSeqs[j] })
	return status
}

// StoredTelemetry returns the records held for a device ordered by seq.
func (r *Registry) StoredTelemetry(id protocol.DeviceID) []protocol.TelemetryRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[id]
	if !ok {
		return nil
	}
	seqs := make([]int64, 0, len(d.telemetry.stored))
	for s := range d.telemetry.stored {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	out := make([]protocol.TelemetryRecord, 0, len(seqs))
	for _, s := range seqs {
		out = append(out, d.telemetry.stored[s])
	}
	return out
}

// MarkedSeqs returns seqs currently flagged gap/out-of-order for a device.
func (r *Registry) MarkedSeqs(id protocol.DeviceID) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[id]
	if !ok {
		return nil
	}
	out := make([]int64, 0, len(d.telemetry.markedGap))
	for s := range d.telemetry.markedGap {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TelemetryWatermark returns next expected contiguous seq.
func (r *Registry) TelemetryWatermark(id protocol.DeviceID) (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[id]
	if !ok {
		return 0, false
	}
	return d.telemetry.nextSeq, true
}

// IngestUnknownOK is a convenience that also reports whether the device exists.
func (r *Registry) IngestTelemetryChecked(batch protocol.TelemetryBatch) (protocol.TelemetryStatus, error) {
	if len(batch.Records) == 0 {
		return protocol.TelemetryStatus{}, nil
	}
	if !r.IsRegistered(batch.Records[0].DeviceID) {
		return protocol.TelemetryStatus{}, protocol.NewReject(protocol.RejectUnknownDevice,
			"telemetry from unregistered device %s", batch.Records[0].DeviceID)
	}
	return r.IngestTelemetry(batch), nil
}
