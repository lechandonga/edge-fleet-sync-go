package fleet_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/lechandonga/edge-fleet-sync-go/clock"
	"github.com/lechandonga/edge-fleet-sync-go/fleet"
	"github.com/lechandonga/edge-fleet-sync-go/protocol"
)

// bizLogf prints a single, greppable line per business case so `go test -v`
// shows which business flow each case covers.
func bizLogf(t *testing.T, business, name, format string, args ...any) {
	t.Helper()
	t.Logf("[biz=%s case=%s] %s", business, name, fmt.Sprintf(format, args...))
}

const (
	leaseTTL = 300
	hbEvery  = 100
)

func newFleet(t *testing.T) (*fleet.Fleet, *clock.FakeClock) {
	t.Helper()
	c := clock.NewFakeClock(1_000)
	f := fleet.New(c, leaseTTL, hbEvery)
	return f, c
}

func mustRegister(t *testing.T, f *fleet.Fleet, id protocol.DeviceID, key string, fw int64, cap int) {
	t.Helper()
	if _, err := f.Register(id, key, fw, cap); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
}

func code(err error) protocol.RejectCode {
	if err == nil {
		return ""
	}
	if rj, ok := err.(*protocol.RejectError); ok {
		return rj.Code
	}
	return "other"
}

// ---------- Business: registration & identity ----------

func TestRegister_DuplicateAndConflict(t *testing.T) {
	f, _ := newFleet(t)
	id := protocol.DeviceID("dev-1")

	if err := f.Cloud.Register(id, "keyA", 1); err != nil {
		t.Fatalf("first register: %v", err)
	}
	bizLogf(t, "register", "first-ok", "device %s registered", id)

	err := f.Cloud.Register(id, "keyA", 1)
	if code(err) != protocol.RejectDuplicateRegistration {
		t.Fatalf("want duplicate_registration, got %v", err)
	}
	bizLogf(t, "register", "duplicate-same-key", "reject=%s (idempotent no-op)", code(err))

	err = f.Cloud.Register(id, "keyB", 1)
	if code(err) != protocol.RejectIdentityConflict {
		t.Fatalf("want identity_conflict, got %v", err)
	}
	bizLogf(t, "register", "identity-conflict", "reject=%s", code(err))
}

// ---------- Business: heartbeat, lease expiry, stale lease reconnect ----------

func TestLease_ExpireOfflineAndStaleReconnect(t *testing.T) {
	f, c := newFleet(t)
	id := protocol.DeviceID("dev-hb")
	mustRegister(t, f, id, "k", 1, 10)
	dev := f.Device(id)

	// Heartbeats keep the lease alive while time advances under TTL.
	c.Advance(hbEvery)
	f.RunOnce()
	if snap, _ := f.Cloud.Snapshot(id); !snap.Online {
		t.Fatal("device should be online after heartbeat")
	}
	bizLogf(t, "lease", "heartbeat-renews", "t=%d online=true", c.NowMs())

	// Stop sending heartbeats; after TTL the cloud must expire the lease.
	c.Advance(leaseTTL + 1)
	res := f.RunOnce()
	if len(res.Expired) != 1 || res.Expired[0] != id {
		t.Fatalf("want exactly %s expired, got %v", id, res.Expired)
	}
	if snap, _ := f.Cloud.Snapshot(id); snap.Online {
		t.Fatal("device must be offline after lease expiry")
	}
	bizLogf(t, "lease", "expire-sweep", "t=%d expired=%v", c.NowMs(), res.Expired)

	// An old-lease heartbeat must not resurrect the device.
	oldLease := dev.Lease()
	hb := protocol.Heartbeat{Epoch: oldLease.Epoch, Counter: 99, NowMs: c.NowMs()}
	_, err := f.Cloud.Heartbeat(id, oldLease, hb, c)
	if code(err) != protocol.RejectStaleLease {
		t.Fatalf("want stale_lease for expired session, got %v", err)
	}
	bizLogf(t, "lease", "old-lease-rejected", "reject=%s", code(err))

	// Pushing to an expired device queues instead of live delivery.
	before := f.Trans.CloudPushCount()
	if _, err := f.Cloud.PushConfig(id, protocol.Config{Version: 1, FirmwareMin: 1, FirmwareMax: 1, Body: "queued"}); err != nil {
		t.Fatal(err)
	}
	if f.Trans.CloudPushCount() != before {
		t.Fatal("no live delivery allowed while offline")
	}
	bizLogf(t, "lease", "offline-no-new-delivery", "push queued, pending=%v", f.Cloud.PendingConfigs(id))

	// Reconnect mints a new epoch; queued push flushes and converges.
	if err := f.Reconnect(id); err != nil {
		t.Fatal(err)
	}
	snap, _ := f.Cloud.Snapshot(id)
	if !snap.Online || snap.Epoch != oldLease.Epoch+1 {
		t.Fatalf("want new epoch online, got %+v", snap)
	}
	f.RunOnce()
	if dev.AppliedVersion() != 1 {
		t.Fatalf("queued config should apply after reconnect, got v%d", dev.AppliedVersion())
	}
	if snap2, _ := f.Cloud.Snapshot(id); snap2.AppliedVersion != 1 || snap2.PendingCount != 0 {
		t.Fatalf("cloud state not converged: %+v", snap2)
	}
	bizLogf(t, "lease", "reconnect-flush", "new epoch=%d applied=v1 pending=0", snap.Epoch)
}

func TestHeartbeat_OutOfOrderCounter(t *testing.T) {
	f, c := newFleet(t)
	id := protocol.DeviceID("dev-ooo")
	mustRegister(t, f, id, "k", 1, 10)
	dev := f.Device(id)

	hb2 := protocol.Heartbeat{Epoch: dev.Lease().Epoch, Counter: 2, NowMs: c.NowMs()}
	if _, err := f.Cloud.Heartbeat(id, dev.Lease(), hb2, c); err != nil {
		t.Fatal(err)
	}
	hb1 := protocol.Heartbeat{Epoch: dev.Lease().Epoch, Counter: 1, NowMs: c.NowMs()}
	_, err := f.Cloud.Heartbeat(id, dev.Lease(), hb1, c)
	if code(err) != protocol.RejectHeartbeatOrder {
		t.Fatalf("want heartbeat_out_of_order, got %v", err)
	}
	// Same counter redelivered is also rejected.
	_, err = f.Cloud.Heartbeat(id, dev.Lease(), hb2, c)
	if code(err) != protocol.RejectHeartbeatOrder {
		t.Fatalf("duplicate counter must reject, got %v", err)
	}
	bizLogf(t, "lease", "counter-order", "counter 2 then 1/dup rejected=%s", code(err))
}

// ---------- Business: config versioning, compatibility, redelivery ----------

func TestConfig_VersionRulesAndRedelivery(t *testing.T) {
	f, c := newFleet(t)
	id := protocol.DeviceID("dev-cfg")
	mustRegister(t, f, id, "k", 2, 10)
	dev := f.Device(id)

	// v1 applies online.
	if _, err := f.Cloud.PushConfig(id, protocol.Config{Version: 1, FirmwareMin: 1, FirmwareMax: 2, Body: "A"}); err != nil {
		t.Fatal(err)
	}
	f.RunOnce()
	if dev.AppliedVersion() != 1 || dev.AppliedBody() != "A" {
		t.Fatalf("v1 not applied: v=%d body=%q", dev.AppliedVersion(), dev.AppliedBody())
	}
	bizLogf(t, "config", "v1-applied", "desired=1 applied=1")

	// Rollback refused.
	var err error
	_, err = f.Cloud.PushConfig(id, protocol.Config{Version: 1, FirmwareMin: 1, FirmwareMax: 2})
	if err != nil {
		t.Fatalf("identical re-push must be idempotent, got %v", err)
	}
	_, err = f.Cloud.PushConfig(id, protocol.Config{Version: 0, FirmwareMin: 1, FirmwareMax: 2})
	if code(err) != protocol.RejectConfigRollback {
		t.Fatalf("want rollback reject, got %v", err)
	}
	bizLogf(t, "config", "rollback-rejected", "reject=%s", code(err))

	// Incompatible firmware window refused.
	_, err = f.Cloud.PushConfig(id, protocol.Config{Version: 2, FirmwareMin: 3, FirmwareMax: 5})
	if code(err) != protocol.RejectConfigIncompatible {
		t.Fatalf("want incompatible reject, got %v", err)
	}
	bizLogf(t, "config", "incompatible-rejected", "reject=%s fw=device:2", code(err))

	// Simulate lost ack: cloud still considers v1 desired; redeliver same
	// version and ensure edge does not double-apply and cloud converges.
	env, err := f.Cloud.PushConfig(id, protocol.Config{Version: 1, FirmwareMin: 1, FirmwareMax: 2, Body: "A"})
	if err != nil || env == nil {
		t.Fatalf("idempotent re-push should return envelope, got %v", err)
	}
	applyCountBefore := dev.ApplyCountForTest()
	if err := dev.ApplyConfigPush(env); err != nil {
		t.Fatal(err)
	}
	if dev.ApplyCountForTest() != applyCountBefore {
		t.Fatal("redelivered config must not apply twice")
	}
	f.RunOnce()
	snap, _ := f.Cloud.Snapshot(id)
	if snap.DesiredVersion != 1 || snap.AppliedVersion != 1 {
		t.Fatalf("states diverged after redelivery: %+v", snap)
	}
	bizLogf(t, "config", "redelivery-idempotent", "apply-once=%t applied=v1 pending=0",
		dev.ApplyCountForTest() == applyCountBefore)

	// Offline queuing: disconnect, push v2/v3, reconnect -> FIFO apply.
	if err := f.Disconnect(id); err != nil {
		t.Fatal(err)
	}
	c.Advance(leaseTTL + 1)
	f.RunOnce()
	for _, vb := range [][2]string{{"2", "B"}, {"3", "C"}} {
		var v int64
		fmt.Sscanf(vb[0], "%d", &v)
		if _, err := f.Cloud.PushConfig(id, protocol.Config{Version: v, FirmwareMin: 1, FirmwareMax: 2, Body: vb[1]}); err != nil {
			t.Fatal(err)
		}
	}
	got := f.Cloud.PendingConfigs(id)
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("want FIFO pending [2 3], got %v", got)
	}
	bizLogf(t, "config", "offline-queued", "pending=%v", got)

	if err := f.Reconnect(id); err != nil {
		t.Fatal(err)
	}
	f.RunOnce()
	if dev.AppliedVersion() != 3 || dev.AppliedBody() != "C" {
		t.Fatalf("want v3/C after FIFO flush, got v%d/%q", dev.AppliedVersion(), dev.AppliedBody())
	}
	if snap2, _ := f.Cloud.Snapshot(id); snap2.PendingCount != 0 || snap2.AppliedVersion != 3 {
		t.Fatalf("cloud did not converge: %+v", snap2)
	}
	bizLogf(t, "config", "reconnect-fifo", "applied=v3 pending=0")

	// Duplicate ack must be harmless.
	if err := f.Cloud.AckConfig(id, protocol.ConfigAck{Version: 3, Epoch: dev.Lease().Epoch}); err != nil {
		t.Fatal(err)
	}
	snap3, _ := f.Cloud.Snapshot(id)
	if snap3.AppliedVersion != 3 || snap3.PendingCount != 0 {
		t.Fatalf("duplicate ack destabilized state: %+v", snap3)
	}
	bizLogf(t, "config", "dup-ack", "applied stays v3")
}

// ---------- Business: telemetry resume, dedup, gaps ----------

func TestTelemetry_OfflineResumeDedupAndGaps(t *testing.T) {
	f, c := newFleet(t)
	id := protocol.DeviceID("dev-tel")
	mustRegister(t, f, id, "k", 1, 100)
	dev := f.Device(id)

	// Online: seq 1..3 upload cleanly.
	for i := 0; i < 3; i++ {
		dev.Record(c.NowMs(), "m", float64(i))
	}
	f.RunOnce()
	if wm, _ := f.Cloud.TelemetryWatermark(id); wm != 4 {
		t.Fatalf("want watermark 4, got %d", wm)
	}
	bizLogf(t, "telemetry", "online-ingest", "seqs 1-3 stored, next=%d", 4)

	// Link down: keep producing. Uplink failure must not block or error the
	// producer; records stay in the bounded buffer.
	f.Trans.SetUplinkDown(id, true)
	if err := f.Disconnect(id); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		dev.Record(c.NowMs(), "m", float64(10+i))
	}
	if n := dev.BufferedCount(); n != 2 {
		t.Fatalf("want 2 buffered during outage, got %d", n)
	}
	if _, err := dev.FlushTelemetry(); err == nil {
		t.Fatal("flush during backend outage should report error but keep buffer")
	}
	if dev.BufferedCount() != 2 {
		t.Fatal("backend outage must not lose buffered records")
	}
	bizLogf(t, "telemetry", "outage-buffers", "buffered=2 flush retried safely")

	// Reconnect: same seqs 4..5 resume in order.
	f.Trans.SetUplinkDown(id, false)
	c.Advance(leaseTTL + 1)
	f.RunOnce()
	if err := f.Reconnect(id); err != nil {
		t.Fatal(err)
	}
	f.RunOnce()
	if wm, _ := f.Cloud.TelemetryWatermark(id); wm != 6 {
		t.Fatalf("want resumed watermark 6, got %d", wm)
	}
	bizLogf(t, "telemetry", "resume-in-order", "seqs 4-5 backfilled, next=6")

	// Duplicate batch (ambiguous send): no double storage.
	storedBefore := len(f.Cloud.StoredTelemetry(id))
	dup := protocol.TelemetryBatch{Records: []protocol.TelemetryRecord{
		{DeviceID: id, Seq: 4, AtMs: 1, Metric: "m", Value: 10},
		{DeviceID: id, Seq: 5, AtMs: 2, Metric: "m", Value: 11},
	}}
	status := f.Cloud.IngestTelemetry(dup)
	if len(status.DupSeqs) != 2 || len(status.AcceptedSeqs) != 0 {
		t.Fatalf("want 2 dups 0 accepted, got %+v", status)
	}
	if len(f.Cloud.StoredTelemetry(id)) != storedBefore {
		t.Fatal("duplicate telemetry must not be stored twice")
	}
	bizLogf(t, "telemetry", "duplicate-suppressed", "dup=%v stored unchanged=%d", status.DupSeqs, storedBefore)

	// Gap: send seq 8 (6,7 missing) then fill 6; 7 must remain marked.
	stGap := f.Cloud.IngestTelemetry(protocol.TelemetryBatch{Records: []protocol.TelemetryRecord{
		{DeviceID: id, Seq: 8, AtMs: 3, Metric: "m", Value: 99},
	}})
	if len(stGap.MissingSeqs) != 2 || stGap.MissingSeqs[0] != 6 || stGap.MissingSeqs[1] != 7 {
		t.Fatalf("want missing [6 7], got %+v", stGap)
	}
	f.Cloud.IngestTelemetry(protocol.TelemetryBatch{Records: []protocol.TelemetryRecord{
		{DeviceID: id, Seq: 6, AtMs: 4, Metric: "m", Value: 12},
	}})
	marked := f.Cloud.MarkedSeqs(id)
	if len(marked) != 1 || marked[0] != 7 { // 7 still missing; 8 is normal once contiguous
		t.Fatalf("want marked {7,8}, got %v", marked)
	}
	bizLogf(t, "telemetry", "gap-marked-not-dropped", "missing-marks=%v (8 retained, 6 backfilled)", marked)

	// Out-of-order late arrival below watermark is stored and flagged.
	stOOO := f.Cloud.IngestTelemetry(protocol.TelemetryBatch{Records: []protocol.TelemetryRecord{
		{DeviceID: id, Seq: 2, AtMs: 5, Metric: "late", Value: 1},
	}})
	if len(stOOO.DupSeqs) != 1 {
		t.Fatalf("seq 2 already stored => dup, got %+v", stOOO)
	}
	bizLogf(t, "telemetry", "late-dup", "seq2 duplicate=%v", stOOO.DupSeqs)
}

func TestTelemetry_CacheOverflowDeterministic(t *testing.T) {
	f, c := newFleet(t)
	id := protocol.DeviceID("dev-cap")
	mustRegister(t, f, id, "k", 1, 3)
	dev := f.Device(id)

	// Disconnect and overfill: capacity 3, produce 5 -> oldest seqs 1,2 evicted
	// by the documented drop_oldest policy.
	f.Trans.SetUplinkDown(id, true)
	if err := f.Disconnect(id); err != nil {
		t.Fatal(err)
	}
	var evictedAll []int64
	for i := 0; i < 5; i++ {
		_, ev := dev.Record(c.NowMs(), "m", float64(i))
		evictedAll = append(evictedAll, ev...)
	}
	if dev.BufferedCount() != 3 {
		t.Fatalf("buffer must stay at capacity 3, got %d", dev.BufferedCount())
	}
	if got := dev.DroppedSeqs(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("drop_oldest must evict [1 2], got %v", got)
	}
	bizLogf(t, "telemetry", "overflow-drop-oldest", "buffered=3 evicted=%v audit=%v",
		evictedAll, dev.DroppedSeqs())

	// Reconnect uploads survivors 3,4,5; cloud flags exactly 1,2 missing.
	f.Trans.SetUplinkDown(id, false)
	if err := f.Reconnect(id); err != nil {
		t.Fatal(err)
	}
	f.RunOnce()
	stored := f.Cloud.StoredTelemetry(id)
	if len(stored) != 3 || stored[0].Seq != 3 {
		t.Fatalf("want survivors [3 4 5], got %v", seqsOf(stored))
	}
	if marked := f.Cloud.MarkedSeqs(id); len(marked) != 2 || marked[0] != 1 || marked[1] != 2 {
		t.Fatalf("evicted seqs must show as missing/marked, got %v", marked)
	}
	bizLogf(t, "telemetry", "overflow-explained", "stored=%v cloud-missing=%v",
		seqsOf(stored), f.Cloud.MarkedSeqs(id))
}

func seqsOf(recs []protocol.TelemetryRecord) []int64 {
	out := make([]int64, len(recs))
	for i, r := range recs {
		out[i] = r.Seq
	}
	return out
}

// ---------- Business: concurrent access ----------

func TestConcurrent_ManyDevicesWritersAndTicks(t *testing.T) {
	f, c := newFleet(t)
	const nDevices = 12
	const nWriters = 4
	const nRecord = 50

	for i := 0; i < nDevices; i++ {
		id := protocol.DeviceID(fmt.Sprintf("dev-%02d", i))
		mustRegister(t, f, id, "k", 1, 10000)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var tickWg sync.WaitGroup

	// Concurrent producers per device.
	for i := 0; i < nDevices; i++ {
		id := protocol.DeviceID(fmt.Sprintf("dev-%02d", i))
		dev := f.Device(id)
		for w := 0; w < nWriters; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for r := 0; r < nRecord; r++ {
					dev.Record(c.NowMs(), "m", float64(r))
				}
			}()
		}
	}

	// Concurrently hammer ticks, heartbeats, config pushes, reconnects.
	tickWg.Add(1)
	go func() {
		defer tickWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				f.RunOnce()
				c.Advance(10)
			}
		}
	}()

	for push := 0; push < 10; push++ {
		for i := 0; i < nDevices; i++ {
			id := protocol.DeviceID(fmt.Sprintf("dev-%02d", i))
			v := int64(push + 1)
			_, _ = f.Cloud.PushConfig(id, protocol.Config{Version: v, FirmwareMin: 1, FirmwareMax: 1, Body: "x"})
		}
		if push%3 == 1 {
			for i := 0; i < nDevices; i++ {
				id := protocol.DeviceID(fmt.Sprintf("dev-%02d", i))
				_ = f.Reconnect(id)
			}
		}
	}

	wg.Wait()
	close(stop)
	tickWg.Wait()

	// Determinism check: every device assigned exactly nWriters*nRecord unique
	// seqs edge-side, and after a quiet tick the cloud holds them with no dups.
	c.Advance(leaseTTL * 10)
	for i := 0; i < nDevices; i++ {
		id := protocol.DeviceID(fmt.Sprintf("dev-%02d", i))
		if err := f.Reconnect(id); err != nil {
			t.Fatal(err)
		}
	}
	for k := 0; k < 5; k++ {
		f.RunOnce()
		c.Advance(hbEvery)
	}
	total := 0
	for i := 0; i < nDevices; i++ {
		id := protocol.DeviceID(fmt.Sprintf("dev-%02d", i))
		stored := f.Cloud.StoredTelemetry(id)
		seen := map[int64]bool{}
		for _, r := range stored {
			if seen[r.Seq] {
				t.Fatalf("%s: duplicate stored seq %d under concurrency", id, r.Seq)
			}
			seen[r.Seq] = true
		}
		total += len(stored)
	}
	want := nDevices * nWriters * nRecord
	if total != want {
		t.Fatalf("want %d unique records cloud-side, got %d", want, total)
	}
	bizLogf(t, "concurrency", "no-lost-or-dup", "devices=%d writers=%d records/dev=%d cloud-total=%d",
		nDevices, nWriters, nWriters*nRecord, total)
}
