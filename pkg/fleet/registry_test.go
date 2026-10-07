package fleet

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestRegistry() (*Registry, *ManualClock) {
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	return NewRegistry(clock, 30*time.Second), clock
}

func TestLeaseExpiryAndStaleLeaseReconnect(t *testing.T) {
	t.Log("业务: 心跳与租约管理 - 租约过期判离线, 旧租约回连不得覆盖新状态")
	reg, clock := newTestRegistry()
	lease, err := reg.Register("dev-1", "token-a")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if !reg.Online("dev-1") {
		t.Fatal("device should be online after register")
	}

	clock.Advance(31 * time.Second) // lease TTL = 30s
	if reg.Online("dev-1") {
		t.Fatal("device must be judged offline after heartbeat timeout")
	}
	if err := reg.Heartbeat("dev-1", lease.ID, 1); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}

	// Device re-registers and gets a new lease; a late heartbeat carrying the
	// old lease must be rejected as stale and must not clobber the new state.
	newLease, err := reg.Register("dev-1", "token-a")
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if newLease.ID == lease.ID {
		t.Fatal("re-registration must issue a fresh lease")
	}
	if err := reg.Heartbeat("dev-1", lease.ID, 2); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("expected ErrStaleLease, got %v", err)
	}
	if reg.CurrentLease("dev-1") != newLease.ID || !reg.Online("dev-1") {
		t.Fatal("stale lease reconnect must not overwrite current valid state")
	}
}

func TestHeartbeatOutOfOrder(t *testing.T) {
	t.Log("业务: 心跳与租约管理 - 乱序心跳给出可区分拒绝, 设备保持在线")
	reg, _ := newTestRegistry()
	lease, _ := reg.Register("dev-1", "token-a")
	if err := reg.Heartbeat("dev-1", lease.ID, 5); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := reg.Heartbeat("dev-1", lease.ID, 3); !errors.Is(err, ErrHeartbeatOutOfOrder) {
		t.Fatalf("expected ErrHeartbeatOutOfOrder, got %v", err)
	}
	if err := reg.Heartbeat("dev-1", lease.ID, 5); !errors.Is(err, ErrHeartbeatOutOfOrder) {
		t.Fatalf("replayed seq must be rejected, got %v", err)
	}
	if !reg.Online("dev-1") {
		t.Fatal("out-of-order heartbeat must not take device offline")
	}
}

func TestDuplicateAndConflictingRegistration(t *testing.T) {
	t.Log("业务: 设备注册 - 重复注册与身份冲突给出可区分拒绝原因")
	reg, _ := newTestRegistry()
	if _, err := reg.Register("dev-1", "token-a"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := reg.Register("dev-1", "token-a"); !errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("expected ErrAlreadyRegistered, got %v", err)
	}
	if _, err := reg.Register("dev-1", "token-b"); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("expected ErrIdentityConflict, got %v", err)
	}
}

func TestConcurrentRegistration(t *testing.T) {
	t.Log("业务: 设备注册 - 并发接入下状态不错乱, 恰好一个注册成功")
	reg, _ := newTestRegistry()
	const n = 64
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := reg.Register("dev-1", fmt.Sprintf("token-%d", i%2))
			mu.Lock()
			if err == nil {
				succeeded++
			} else if !errors.Is(err, ErrAlreadyRegistered) && !errors.Is(err, ErrIdentityConflict) {
				t.Errorf("unexpected error: %v", err)
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if succeeded != 1 {
		t.Fatalf("exactly one registration must win, got %d", succeeded)
	}
	if !reg.Online("dev-1") {
		t.Fatal("device must be online after concurrent registration")
	}
	// Concurrent heartbeats from the winner stay consistent.
	var wg2 sync.WaitGroup
	leaseID := reg.CurrentLease("dev-1")
	for i := 1; i <= n; i++ {
		wg2.Add(1)
		go func(seq uint64) {
			defer wg2.Done()
			_ = reg.Heartbeat("dev-1", leaseID, seq)
		}(uint64(i))
	}
	wg2.Wait()
	if !reg.Online("dev-1") {
		t.Fatal("device must remain online after concurrent heartbeats")
	}
}
