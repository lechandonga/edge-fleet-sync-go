package fleet

import "time"

// Clock abstracts time so tests can advance it manually.
type Clock interface {
	Now() time.Time
}

// RealClock uses wall time.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// ManualClock is controlled by tests; no real waiting is needed.
type ManualClock struct {
	now time.Time
}

func NewManualClock(start time.Time) *ManualClock { return &ManualClock{now: start} }

func (c *ManualClock) Now() time.Time { return c.now }

func (c *ManualClock) Advance(d time.Duration) { c.now = c.now.Add(d) }
