package protocol

import "fmt"

// DeviceID identifies an edge device inside the fleet.
type DeviceID string

// RejectCode gives callers a machine-readable reason for a refused request.
type RejectCode string

const (
	// RejectDuplicateRegistration: device id is already registered (re-register
	// with the same key is a no-op success; a different key is an identity
	// conflict, see RejectIdentityConflict).
	RejectDuplicateRegistration RejectCode = "duplicate_registration"
	// RejectIdentityConflict: same device id presented with a different key.
	RejectIdentityConflict RejectCode = "identity_conflict"
	// RejectUnknownDevice: cloud has no record of the device.
	RejectUnknownDevice RejectCode = "unknown_device"
	// RejectStaleLease: heartbeat/connect carries a lease that has already
	// been superseded (renewed by a newer session or expired).
	RejectStaleLease RejectCode = "stale_lease"
	// RejectDeviceOffline: the device lease is expired; new pushes are queued
	// rather than delivered, but tasks that require live delivery are refused.
	RejectDeviceOffline RejectCode = "device_offline"
	// RejectHeartbeatOrder: heartbeat epoch/seq is older than the one the
	// cloud last accepted.
	RejectHeartbeatOrder RejectCode = "heartbeat_out_of_order"
	// RejectConfigRollback: new version is not greater than the device version.
	RejectConfigRollback RejectCode = "config_version_rollback"
	// RejectConfigIncompatible: payload incompatibility rules reject the push.
	RejectConfigIncompatible RejectCode = "config_incompatible"
	// RejectConfigStaleAck: ack points at a version the cloud never issued to
	// this device, or comes from a session that predates the current one.
	RejectConfigStaleAck RejectCode = "config_ack_stale"
)

// RejectError carries a stable code plus a human-readable explanation.
type RejectError struct {
	Code   RejectCode
	Reason string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Reason)
}

// NewReject builds a *RejectError.
func NewReject(code RejectCode, format string, args ...any) *RejectError {
	return &RejectError{Code: code, Reason: fmt.Sprintf(format, args...)}
}

// Lease is issued at registration/connect and must be echoed on every
// heartbeat. Each successful (re)connect mints a strictly increasing epoch,
// which invalidates every older lease.
type Lease struct {
	DeviceID  DeviceID
	Epoch     int64
	ExpiresAt int64 // fake-clock milliseconds
}

// Envelope wraps every cloud<->edge request. Deliveries are idempotent on the
// receiver side keyed by (DeviceID, Kind, DedupKey), so a redelivered envelope
// must carry the same DedupKey.
type Envelope struct {
	ID       string
	DeviceID DeviceID
	Kind     MessageKind
	DedupKey string
	Payload  any
}

type MessageKind string

const (
	KindRegister   MessageKind = "register"
	KindHeartbeat  MessageKind = "heartbeat"
	KindConfigPush MessageKind = "config_push"
	KindConfigAck  MessageKind = "config_ack"
	KindTelemetry  MessageKind = "telemetry"
)

// Heartbeat is sent periodically while a device holds a live link.
type Heartbeat struct {
	Epoch   int64
	Counter int64 // per-epoch monotonically increasing counter
	NowMs   int64
}

// Config is a versioned payload. Versions are strictly increasing per device.
// MinDeviceVersion / MaxDeviceVersion describe the firmware-version range the
// config is compatible with.
type Config struct {
	Version     int64
	FirmwareMin int64
	FirmwareMax int64
	Body        string
}

// ConfigPush is the cloud->edge delivery of one Config.
type ConfigPush struct {
	Config          Config
	FirmwareVersion int64 // snapshot of the device firmware the push targeted
}

// ConfigAck is the edge->cloud confirmation.
type ConfigAck struct {
	Version int64
	Epoch   int64 // session in which the config became effective
}

// TelemetryRecord is one sensor reading. Seq is assigned by the edge device
// and is gap-free and increasing across the whole device lifetime (including
// offline periods); redelivery reuses the same Seq.
type TelemetryRecord struct {
	DeviceID DeviceID
	Seq      int64
	AtMs     int64
	Metric   string
	Value    float64
}

// TelemetryBatch is a telemetry uplink frame.
type TelemetryBatch struct {
	Records []TelemetryRecord
}

// TelemetryStatus is the cloud result for one batch.
type TelemetryStatus struct {
	AcceptedSeqs []int64 // newly stored
	DupSeqs      []int64 // already present, not stored again
	MissingSeqs  []int64 // holes detected below the high-water mark
	OutOfOrder   []int64 // records arriving below the expected next seq
	Marked       []int64 // records flagged (gap/out-of-order) but still stored
}
