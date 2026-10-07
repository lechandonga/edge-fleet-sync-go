package transport

import (
	"errors"

	"github.com/lechandonga/edge-fleet-sync-go/protocol"
)

// ErrBackendUnavailable is returned to the edge when its uplink is down.
var ErrBackendUnavailable = errors.New("backend temporarily unavailable")

// DeviceUplink adapts InProc to the edge.Uplink interface for one device.
type DeviceUplink struct {
	T  *InProc
	ID protocol.DeviceID
}

func (u DeviceUplink) SendTelemetry(batch protocol.TelemetryBatch) (protocol.TelemetryStatus, error) {
	u.T.mu.Lock()
	down := u.T.uplinkDown[u.ID]
	u.T.telemetryBatches++
	u.T.mu.Unlock()
	if down {
		return protocol.TelemetryStatus{}, ErrBackendUnavailable
	}
	return u.T.registry.IngestTelemetry(batch), nil
}

func (u DeviceUplink) SendConfigAck(id protocol.DeviceID, ack protocol.ConfigAck) error {
	u.T.mu.Lock()
	down := u.T.uplinkDown[u.ID]
	u.T.configAcks = append(u.T.configAcks, ack.Version)
	u.T.mu.Unlock()
	if down {
		return ErrBackendUnavailable
	}
	return u.T.registry.AckConfig(id, ack)
}
