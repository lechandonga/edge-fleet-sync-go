// Command fleetdemo runs an in-process cloud + three edge devices through a
// scripted lifecycle: online heartbeats, an offline window with queued config
// and buffered telemetry, lease expiry, reconnect/FIFO flush, and a
// duplicate-delivery scenario. No network or credentials are involved.
package main

import (
	"fmt"

	"github.com/lechandonga/edge-fleet-sync-go/clock"
	"github.com/lechandonga/edge-fleet-sync-go/fleet"
	"github.com/lechandonga/edge-fleet-sync-go/protocol"
)

func snap(f *fleet.Fleet, id protocol.DeviceID) string {
	s, ok := f.Cloud.Snapshot(id)
	if !ok {
		return "(unknown)"
	}
	state := "OFFLINE"
	if s.Online {
		state = "online "
	}
	return fmt.Sprintf("%s epoch=%d desired=v%d applied=v%d pending=%d telemetry=%d",
		state, s.Epoch, s.DesiredVersion, s.AppliedVersion, s.PendingCount,
		len(f.Cloud.StoredTelemetry(id)))
}

func main() {
	c := clock.NewFakeClock(1_000)
	const ttl, every = int64(300), int64(100)
	f := fleet.New(c, ttl, every)

	ids := []protocol.DeviceID{"sensor-a", "sensor-b", "sensor-c"}
	for _, id := range ids {
		if _, err := f.Register(id, "secret", 2, 8); err != nil {
			panic(err)
		}
	}
	fmt.Println("== registered 3 devices, all online ==")

	// Normal life: a few ticks, config v1 to everyone.
	for i := 0; i < 2; i++ {
		c.Advance(every)
		f.RunOnce()
	}
	for _, id := range ids {
		_, _ = f.Cloud.PushConfig(id, protocol.Config{Version: 1, FirmwareMin: 1, FirmwareMax: 3, Body: "baseline"})
		f.Device(id).Record(c.NowMs(), "temp", 20.0)
	}
	f.RunOnce()

	// sensor-b loses its link; cloud lease expires; configs v2,v3 queue.
	f.MarkOffline("sensor-b")
	f.Trans.SetUplinkDown("sensor-b", true)
	// The other devices keep sending heartbeats through the outage window;
	// sensor-b misses all of them and is the only one that expires.
	for step := 0; step < 4; step++ {
		c.Advance(every)
		f.RunOnce()
	}
	_, _ = f.Cloud.PushConfig("sensor-b", protocol.Config{Version: 2, FirmwareMin: 1, FirmwareMax: 3, Body: "canary"})
	_, _ = f.Cloud.PushConfig("sensor-b", protocol.Config{Version: 3, FirmwareMin: 1, FirmwareMax: 3, Body: "stable"})
	for i := 0; i < 3; i++ {
		f.Device("sensor-b").Record(c.NowMs(), "temp", 21.0+float64(i))
	}
	fmt.Println("== sensor-b link lost, lease expired; v2/v3 queued, telemetry buffered ==")

	// Others keep ticking (their leases stay alive).
	c.Advance(every)
	f.RunOnce()

	// sensor-b reconnects: new epoch, FIFO config flush, buffered telemetry.
	f.Trans.SetUplinkDown("sensor-b", false)
	if err := f.Reconnect("sensor-b"); err != nil {
		panic(err)
	}
	f.RunOnce()
	fmt.Println("== sensor-b reconnected: queued configs flushed FIFO, telemetry backfilled ==")

	// Duplicate config delivery (lost-ack retry) changes nothing.
	env, _ := f.Cloud.PushConfig("sensor-b", protocol.Config{Version: 3, FirmwareMin: 1, FirmwareMax: 3, Body: "stable"})
	_ = f.Device("sensor-b").ApplyConfigPush(env)
	_ = f.Device("sensor-b").FlushAcks()
	fmt.Println("== duplicate v3 delivery retried: no double apply ==")

	for _, id := range ids {
		fmt.Printf("device %-9s %s\n", id, snap(f, id))
	}
	fmt.Printf("sensor-b dropped seqs (overflow audit): %v\n", f.Device("sensor-b").DroppedSeqs())
	fmt.Printf("sensor-b cloud-marked gaps: %v\n", f.Cloud.MarkedSeqs("sensor-b"))
	fmt.Println("demo complete")
}
