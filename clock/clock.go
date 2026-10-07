package clock

import "sync"

// Clock is the minimal time source used by the fleet. Tests use FakeClock so
// no test ever sleeps or waits on wall time.
type Clock interface {
	NowMs() int64
}

// WallClock reads the real Unix time in milliseconds.
type WallClock struct{}

func (WallClock) NowMs() int64 { return timeNowMs() }

// FakeClock is a manually advanced, concurrency-safe clock.
type FakeClock struct {
	mu  sync.Mutex
	now int64
}

func NewFakeClock(startMs int64) *FakeClock { return &FakeClock{now: startMs} }

func (c *FakeClock) NowMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves time forward (or, if you really want, backward) by d ms.
func (c *FakeClock) Advance(d int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now += d
	return c.now
}
