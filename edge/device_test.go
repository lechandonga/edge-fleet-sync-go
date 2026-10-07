package edge_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/lechandonga/edge-fleet-sync-go/edge"
	"github.com/lechandonga/edge-fleet-sync-go/protocol"
)

type fakeUplink struct {
	mu       sync.Mutex
	down     bool
	telCalls int
	ackCalls int
}

func (f *fakeUplink) SendTelemetry(b protocol.TelemetryBatch) (protocol.TelemetryStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.telCalls++
	if f.down {
		return protocol.TelemetryStatus{}, errors.New("backend down")
	}
	st := protocol.TelemetryStatus{}
	for _, r := range b.Records {
		st.AcceptedSeqs = append(st.AcceptedSeqs, r.Seq)
	}
	return st, nil
}

func (f *fakeUplink) SendConfigAck(id protocol.DeviceID, ack protocol.ConfigAck) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackCalls++
	if f.down {
		return errors.New("backend down")
	}
	return nil
}

func TestProducerNeverBlocksWhenBackendDown(t *testing.T) {
	up := &fakeUplink{down: true}
	d := edge.New("d1", "k", 1, 2, up)
	d.Session(protocol.Lease{DeviceID: "d1", Epoch: 1})

	// Buffer capacity 2, write 100: producers never fail or block; evictions
	// follow drop-oldest deterministically.
	for i := 0; i < 100; i++ {
		seq, ev := d.Record(1, "m", float64(i))
		if seq != int64(i+1) {
			t.Fatalf("seq allocation broken: %d", seq)
		}
		_ = ev
	}
	if d.BufferedCount() != 2 {
		t.Fatalf("buffer must hold at most 2, got %d", d.BufferedCount())
	}
	dropped := d.DroppedSeqs()
	if len(dropped) != 98 || dropped[0] != 1 || dropped[97] != 98 {
		t.Fatalf("want evicted 1..98 audit, got len=%d head=%v tail=%v",
			len(dropped), dropped[:min(3, len(dropped))], dropped[len(dropped)-1])
	}

	// Flush fails but retains both survivors; then backend recovers.
	if _, err := d.FlushTelemetry(); err == nil {
		t.Fatal("expected uplink error")
	}
	if d.BufferedCount() != 2 {
		t.Fatal("failed flush must retain buffer")
	}
	up.down = false
	sent, err := d.FlushTelemetry()
	if err != nil || sent != 2 || d.BufferedCount() != 0 {
		t.Fatalf("want 2 sent, empty buffer; got sent=%d err=%v buf=%d", sent, err, d.BufferedCount())
	}
	t.Log("[biz=edge-cache case=never-blocks] 100 writes capacity 2 -> evicted 1..98, survivors flushed after recovery")
}

func TestConfigPushAppliesOnceAcrossRedelivery(t *testing.T) {
	up := &fakeUplink{}
	d := edge.New("d1", "k", 2, 4, up)
	d.Session(protocol.Lease{DeviceID: "d1", Epoch: 1})

	env := &protocol.Envelope{
		ID: "cfg-d1-1", DeviceID: "d1", Kind: protocol.KindConfigPush,
		DedupKey: "config:1",
		Payload:  protocol.ConfigPush{Config: protocol.Config{Version: 1, FirmwareMin: 1, FirmwareMax: 2, Body: "x"}},
	}
	for i := 0; i < 3; i++ {
		if err := d.ApplyConfigPush(env); err != nil {
			t.Fatal(err)
		}
	}
	if d.ApplyCountForTest() != 1 {
		t.Fatalf("must apply once, got %d", d.ApplyCountForTest())
	}
	if d.PendingAckCount() != 1 {
		t.Fatalf("single pending ack, got %d", d.PendingAckCount())
	}
	if err := d.FlushAcks(); err != nil || d.PendingAckCount() != 0 {
		t.Fatalf("ack flush: err=%v pending=%d", err, d.PendingAckCount())
	}
	// Re-deliver after ack: ack already delivered -> stays idempotent.
	if err := d.ApplyConfigPush(env); err != nil {
		t.Fatal(err)
	}
	if d.ApplyCountForTest() != 1 || d.PendingAckCount() != 0 {
		t.Fatalf("redelivery after ack must be a pure no-op")
	}

	// Rollback on the edge is refused too.
	old := &protocol.Envelope{ID: "cfg-d1-0", DeviceID: "d1", Kind: protocol.KindConfigPush,
		DedupKey: "config:0", Payload: protocol.ConfigPush{Config: protocol.Config{Version: 0}}}
	if err := d.ApplyConfigPush(old); err == nil {
		t.Fatal("edge must refuse version rollback")
	}
	t.Log("[biz=edge-config case=apply-once] 3 redeliveries -> 1 effective apply, single ack; rollback refused")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
