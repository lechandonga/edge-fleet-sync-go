package fleet

import (
	"testing"
	"time"
)

func newTelemetryFixture(cacheCap int) *configFixture {
	return newConfigFixture(cacheCap)
}

func TestTelemetryReconnectBackfill(t *testing.T) {
	t.Log("业务: 遥测续传 - 断连期间累积, 恢复后按序补齐")
	f := newTelemetryFixture(100)
	if err := f.dev.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	f.dev.RecordTelemetry("online-1")
	if _, err := f.dev.FlushTelemetry(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	f.dev.Link.SetUp(false)
	for _, p := range []string{"offline-1", "offline-2", "offline-3"} {
		f.dev.RecordTelemetry(p)
	}
	if _, err := f.dev.FlushTelemetry(); err == nil {
		t.Fatal("flush while offline must fail without losing data")
	}
	if got := f.dev.Cache.Len(); got != 3 {
		t.Fatalf("offline records must stay cached, got %d", got)
	}
	f.dev.Link.SetUp(true)
	sent, err := f.dev.FlushTelemetry()
	if err != nil {
		t.Fatalf("flush after reconnect: %v", err)
	}
	if sent != 3 {
		t.Fatalf("sent = %d, want 3", sent)
	}
	recs := f.st.Records()
	if len(recs) != 4 {
		t.Fatalf("stored = %d, want 4", len(recs))
	}
	for i, r := range recs {
		if r.Seq != uint64(i+1) {
			t.Fatalf("record %d seq = %d, want %d (backfill must be in order)", i, r.Seq, i+1)
		}
		if len(r.Flags) != 0 {
			t.Fatalf("record %d unexpectedly flagged %v", i, r.Flags)
		}
	}
}

func TestTelemetryDuplicateAndGapMarking(t *testing.T) {
	t.Log("业务: 遥测入库 - 重复上报去重, 序号缺失与乱序被标记而非丢弃")
	f := newTelemetryFixture(100)
	f.dev.Link.SetDuplicates(1) // every record delivered twice
	f.dev.RecordTelemetry("a")
	f.dev.RecordTelemetry("b")
	if _, err := f.dev.FlushTelemetry(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := len(f.st.Records()); got != 2 {
		t.Fatalf("duplicates must not be stored twice, stored = %d", got)
	}
	if f.st.Duplicates() != 2 {
		t.Fatalf("expected 2 deduplicated deliveries, got %d", f.st.Duplicates())
	}
	// Gap: seq 5 arrives while max is 2.
	if _, err := f.st.Ingest("dev-1", 5, "gap"); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	// Out-of-order: seq 3 arrives after seq 5.
	if _, err := f.st.Ingest("dev-1", 3, "late"); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	recs := f.st.Records()
	if len(recs) != 4 {
		t.Fatalf("anomalous records must be stored, not dropped; stored = %d", len(recs))
	}
	if recs[2].Flags[0] != FlagGap {
		t.Fatalf("seq 5 must be flagged GAP, got %v", recs[2].Flags)
	}
	if recs[3].Flags[0] != FlagOutOfOrder {
		t.Fatalf("seq 3 must be flagged OUT_OF_ORDER, got %v", recs[3].Flags)
	}
}

func TestCacheOverflowDropOldest(t *testing.T) {
	t.Log("业务: 遥测缓存 - 容量上限时确定性丢弃最旧记录且可解释")
	f := newTelemetryFixture(3)
	f.dev.Link.SetUp(false)
	for _, p := range []string{"r1", "r2", "r3", "r4", "r5"} {
		f.dev.RecordTelemetry(p)
	}
	if got := f.dev.Cache.Len(); got != 3 {
		t.Fatalf("cache len = %d, want 3", got)
	}
	if got := f.dev.Cache.Dropped(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}
	f.dev.Link.SetUp(true)
	if _, err := f.dev.FlushTelemetry(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	recs := f.st.Records()
	if len(recs) != 3 || recs[0].Payload != "r3" {
		t.Fatalf("oldest records must be dropped first, got %+v", recs)
	}
	if recs[0].Flags[0] != FlagGap {
		t.Fatalf("first surviving record must be flagged GAP, got %v", recs[0].Flags)
	}
}

func TestCacheWriteNeverBlockedByBackend(t *testing.T) {
	t.Log("业务: 遥测缓存 - 后端不可用时本地写入不阻塞不失败, 恢复后补齐")
	f := newTelemetryFixture(10)
	f.st.SetAvailable(false)
	for i := 0; i < 5; i++ {
		if evicted := f.dev.RecordTelemetry("m"); evicted {
			t.Fatal("no eviction expected under capacity")
		}
	}
	sent, err := f.dev.FlushTelemetry()
	if err != nil || sent != 0 {
		t.Fatalf("flush with backend down must retain data, sent=%d err=%v", sent, err)
	}
	if got := f.dev.Cache.Len(); got != 5 {
		t.Fatalf("records must stay cached while backend down, got %d", got)
	}
	f.st.SetAvailable(true)
	sent, err = f.dev.FlushTelemetry()
	if err != nil || sent != 5 {
		t.Fatalf("flush after recovery, sent=%d err=%v", sent, err)
	}
	if got := len(f.st.Records()); got != 5 {
		t.Fatalf("stored = %d, want 5", got)
	}
}

func TestFlushDoesNotBlockOnSlowBackend(t *testing.T) {
	t.Log("业务: 遥测缓存 - 写入路径不依赖后端, 全程本地完成")
	f := newTelemetryFixture(2)
	f.st.SetAvailable(false)
	start := time.Now()
	for i := 0; i < 1000; i++ {
		f.dev.RecordTelemetry("x")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("cache writes must not block, took %v", elapsed)
	}
	if f.dev.Cache.Dropped() != 998 {
		t.Fatalf("dropped = %d, want 998", f.dev.Cache.Dropped())
	}
}
