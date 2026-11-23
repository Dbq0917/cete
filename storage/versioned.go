package storage

import (
	"bytes"
	"encoding/json"
)

// versionedValueMagic prefixes every value stored by the KVS so that legacy
// values written before versioning was introduced can still be recognized.
const versionedValueMagic = "\x00cete1"

// VersionedValue is a value together with the causal history of the writes
// that produced it.
type VersionedValue struct {
	Value     []byte `json:"value"`
	Clock     VClock `json:"clock"`
	Timestamp int64  `json:"timestamp"` // wall clock of the write, last-write-wins tiebreak
	NodeID    string `json:"node_id"`   // node that performed the write
}

// VersionedKeyValue is a key together with its versioned value, used when
// exchanging data between two replicas for reconciliation.
type VersionedKeyValue struct {
	Key   string
	Value *VersionedValue
}

// Conflict records a genuine concurrent modification detected during
// reconciliation. The losing version is preserved here so that no write is
// ever silently discarded: the conflict report is the audit trail of the
// automatic resolution.
type Conflict struct {
	Key         string
	LocalValue  []byte
	RemoteValue []byte
	WinnerValue []byte
	LocalClock  VClock
	RemoteClock VClock
}

func encodeVersioned(value *VersionedValue) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append([]byte(versionedValueMagic), data...), nil
}

// decodeVersioned unwraps a stored value. The second return value reports
// whether the stored bytes were a versioned envelope; legacy values written
// before versioning was introduced are returned wrapped in a VersionedValue
// with an empty clock (i.e. causally older than any versioned write).
func decodeVersioned(data []byte) (*VersionedValue, bool) {
	if !bytes.HasPrefix(data, []byte(versionedValueMagic)) {
		return &VersionedValue{
			Value: append([]byte{}, data...),
			Clock: VClock{},
		}, false
	}

	value := &VersionedValue{}
	if err := json.Unmarshal(data[len(versionedValueMagic):], value); err != nil {
		return nil, false
	}
	if value.Clock == nil {
		value.Clock = VClock{}
	}
	return value, true
}

// unwrapVersioned decodes a stored value, falling back to treating the raw
// bytes as a legacy unversioned value when they are not a valid envelope.
func unwrapVersioned(data []byte) *VersionedValue {
	if value, ok := decodeVersioned(data); ok && value != nil {
		return value
	}
	return &VersionedValue{
		Value: append([]byte{}, data...),
		Clock: VClock{},
	}
}

// resolve picks the winning version of two replicas of the same key and
// reports whether the two versions are in genuine conflict. A conflict is
// only raised when the writes are causally concurrent (neither version
// happens before the other) or when identical clocks carry different values.
// Causally ordered versions - which includes all writes serialized through a
// single Raft leader - are never reported as conflicts.
//
// The resolution is deterministic and commutative: resolve(a, b) and
// resolve(b, a) always select the same winner, so replicas converge no
// matter in which order reconciliation runs.
func resolve(local *VersionedValue, remote *VersionedValue) (winner *VersionedValue, conflict bool) {
	switch Compare(local.Clock, remote.Clock) {
	case RelationDominates:
		return local, false
	case RelationDominatedBy:
		return remote, false
	case RelationEqual:
		if bytes.Equal(local.Value, remote.Value) {
			return local, false
		}
		// Same causal point but different values: a genuine conflict.
		return tiebreak(local, remote), true
	default: // RelationConcurrent
		return tiebreak(local, remote), true
	}
}

// tiebreak deterministically picks a winner between two conflicting versions
// using last-write-wins semantics: the later timestamp wins, ties are broken
// by node ID and finally by value bytes.
func tiebreak(a *VersionedValue, b *VersionedValue) *VersionedValue {
	if a.Timestamp != b.Timestamp {
		if a.Timestamp > b.Timestamp {
			return a
		}
		return b
	}
	if a.NodeID != b.NodeID {
		if a.NodeID > b.NodeID {
			return a
		}
		return b
	}
	if bytes.Compare(a.Value, b.Value) >= 0 {
		return a
	}
	return b
}
