package fleet

import (
	"errors"
	"testing"
	"time"
)

type configFixture struct {
	reg  *Registry
	cm   *ConfigManager
	st   *TelemetryStore
	dev  *Device
	clk  *ManualClock
}

func newConfigFixture(cacheCap int) *configFixture {
	clk := NewManualClock(time.Unix(1_700_000_000, 0))
	reg := NewRegistry(clk, 30*time.Second)
	cm := NewConfigManager(reg)
	st := NewTelemetryStore()
	dev := NewDevice("dev-1", "token-a", reg, cm, st, cacheCap)
	return &configFixture{reg, cm, st, dev, clk}
}

func TestConfigVersionRules(t *testing.T) {
	t.Log("业务: 配置下发 - 版本只能向前, 回退与不兼容被明确拒绝")
	f := newConfigFixture(0)
	if err := f.dev.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	push := func(v, req uint64) error {
		return f.cm.Push("dev-1", Config{Version: v, RequiresVersion: req, Payload: map[string]string{"k": "v"}})
	}
	if err := push(3, 0); err != nil {
		t.Fatalf("push v3: %v", err)
	}
	if err := f.dev.SyncConfig(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if got := f.cm.AppliedVersion("dev-1"); got != 3 {
		t.Fatalf("applied = %d, want 3", got)
	}
	if err := push(2, 0); !errors.Is(err, ErrVersionRegression) {
		t.Fatalf("regression below applied must be rejected, got %v", err)
	}
	if err := push(3, 0); !errors.Is(err, ErrVersionRegression) {
		t.Fatalf("equal version must be rejected, got %v", err)
	}
	if err := push(4, 5); !errors.Is(err, ErrIncompatibleConfig) {
		t.Fatalf("incompatible config must be rejected, got %v", err)
	}
	if err := push(4, 3); err != nil {
		t.Fatalf("compatible forward version must be accepted, got %v", err)
	}
}

func TestOfflineQueueAndOrderedDelivery(t *testing.T) {
	t.Log("业务: 配置下发 - 离线期间排队, 恢复后按序送达")
	f := newConfigFixture(0)
	if err := f.dev.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	f.dev.Link.SetUp(false) // device goes offline
	for v := uint64(1); v <= 3; v++ {
		if err := f.cm.Push("dev-1", Config{Version: v}); err != nil {
			t.Fatalf("push v%d: %v", v, err)
		}
	}
	if got := f.cm.Pending("dev-1"); got != 3 {
		t.Fatalf("pending = %d, want 3", got)
	}
	if err := f.dev.SyncConfig(); err == nil {
		t.Fatal("offline device must not receive new tasks")
	}
	f.dev.Link.SetUp(true)
	if err := f.dev.Connect(); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if err := f.dev.SyncConfig(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if f.dev.Applied.Version != 3 || f.dev.ApplyCount != 3 {
		t.Fatalf("configs must be applied in order 1,2,3; applied=%d count=%d", f.dev.Applied.Version, f.dev.ApplyCount)
	}
	if got := f.cm.Pending("dev-1"); got != 0 {
		t.Fatalf("queue must drain, pending = %d", got)
	}
}

func TestDuplicateConfigDeliveryIdempotent(t *testing.T) {
	t.Log("业务: 配置下发 - 确认丢失导致重复投递, 不重复生效且两侧一致")
	f := newConfigFixture(0)
	if err := f.dev.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := f.cm.Push("dev-1", Config{Version: 1}); err != nil {
		t.Fatalf("push: %v", err)
	}
	f.dev.Link.SetDuplicates(2) // every delivery arrives 3 times
	if err := f.dev.SyncConfig(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if f.dev.ApplyCount != 1 {
		t.Fatalf("duplicate delivery must not re-apply, applyCount = %d", f.dev.ApplyCount)
	}
	// Simulate lost ack: controller still has the config in flight and
	// redelivers; device acks again without re-applying.
	if err := f.dev.SyncConfig(); err != nil {
		t.Fatalf("resync: %v", err)
	}
	if f.dev.ApplyCount != 1 {
		t.Fatalf("redelivery after ack must stay idempotent, applyCount = %d", f.dev.ApplyCount)
	}
	if got := f.cm.AppliedVersion("dev-1"); got != 1 || f.dev.Applied.Version != 1 {
		t.Fatalf("sides diverged: cloud=%d device=%d", got, f.dev.Applied.Version)
	}
}

func TestOfflineDeviceCannotReceiveTasks(t *testing.T) {
	t.Log("业务: 配置下发 - 心跳超期设备判离线, 不再接收新下发任务")
	f := newConfigFixture(0)
	if err := f.dev.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := f.cm.Push("dev-1", Config{Version: 1}); err != nil {
		t.Fatalf("push: %v", err)
	}
	f.clk.Advance(31 * time.Second) // lease expires
	if _, ok, err := f.cm.NextConfig("dev-1"); err != nil || ok {
		t.Fatalf("expired device must not receive tasks, ok=%v err=%v", ok, err)
	}
	if got := f.cm.Pending("dev-1"); got != 1 {
		t.Fatalf("task must stay queued, pending = %d", got)
	}
}
