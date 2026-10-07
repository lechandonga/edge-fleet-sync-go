package fleet

import (
	"errors"
	"fmt"
	"time"
)

// DeviceID uniquely identifies an edge device.
type DeviceID string

// Lease is issued by the controller on registration. A device is considered
// online only while its lease is unexpired and it keeps heartbeating.
type Lease struct {
	ID        uint64
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// Config is a versioned configuration unit pushed from cloud to edge.
type Config struct {
	// Version must strictly increase per device.
	Version uint64
	// RequiresVersion is the minimum applied version a device must already
	// have for this config to be compatible.
	RequiresVersion uint64
	Payload         map[string]string
}

// TelemetryFlag marks anomalies on stored telemetry records.
type TelemetryFlag string

const (
	// FlagGap marks a record whose sequence implies missing predecessors.
	FlagGap TelemetryFlag = "GAP"
	// FlagOutOfOrder marks a record arriving after a higher sequence was stored.
	FlagOutOfOrder TelemetryFlag = "OUT_OF_ORDER"
)

// TelemetryRecord is a single stored telemetry entry.
type TelemetryRecord struct {
	DeviceID DeviceID
	Seq      uint64
	Payload  string
	Flags    []TelemetryFlag
}

// Distinguishable rejection reasons for registry operations.
var (
	ErrAlreadyRegistered   = errors.New("fleet: device already registered")
	ErrIdentityConflict    = errors.New("fleet: identity conflict for device id")
	ErrUnknownDevice       = errors.New("fleet: unknown device")
	ErrStaleLease          = errors.New("fleet: stale lease")
	ErrLeaseExpired        = errors.New("fleet: lease expired")
	ErrHeartbeatOutOfOrder = errors.New("fleet: heartbeat out of order")
	ErrDeviceOffline       = errors.New("fleet: device is offline")
)

// Config and store rejection reasons.
var (
	ErrVersionRegression  = errors.New("fleet: config version regression")
	ErrIncompatibleConfig = errors.New("fleet: config incompatible with device applied version")
	ErrStoreUnavailable   = errors.New("fleet: telemetry store unavailable")
)

// RejectionError wraps a sentinel reason with device context.
type RejectionError struct {
	Device DeviceID
	Reason error
	Detail string
}

func (e *RejectionError) Error() string {
	return fmt.Sprintf("device %s: %v: %s", e.Device, e.Reason, e.Detail)
}

func (e *RejectionError) Unwrap() error { return e.Reason }

func reject(id DeviceID, reason error, detail string) error {
	return &RejectionError{Device: id, Reason: reason, Detail: detail}
}
