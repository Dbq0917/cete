package storage

import (
	"bytes"
	"testing"
)

func TestResolveCausallyOrdered(t *testing.T) {
	older := &VersionedValue{Value: []byte("v1"), Clock: VClock{"dc1": 1}, Timestamp: 1, NodeID: "dc1"}
	newer := &VersionedValue{Value: []byte("v2"), Clock: VClock{"dc1": 2}, Timestamp: 2, NodeID: "dc1"}

	// a newer version that causally follows the older one is not a conflict,
	// no matter from which side reconciliation looks at it
	winner, conflict := resolve(older, newer)
	if conflict {
		t.Error("causally ordered versions must not be reported as a conflict")
	}
	if winner != newer {
		t.Error("the newer version must win")
	}

	winner, conflict = resolve(newer, older)
	if conflict {
		t.Error("causally ordered versions must not be reported as a conflict")
	}
	if winner != newer {
		t.Error("the newer version must win regardless of argument order")
	}
}

func TestResolveConcurrent(t *testing.T) {
	a := &VersionedValue{Value: []byte("from-dc1"), Clock: VClock{"dc1": 2}, Timestamp: 10, NodeID: "dc1"}
	b := &VersionedValue{Value: []byte("from-dc2"), Clock: VClock{"dc1": 1, "dc2": 1}, Timestamp: 20, NodeID: "dc2"}

	winnerAB, conflictAB := resolve(a, b)
	winnerBA, conflictBA := resolve(b, a)

	if !conflictAB || !conflictBA {
		t.Error("concurrent versions must be reported as a conflict")
	}
	if winnerAB != winnerBA {
		t.Error("resolution must be deterministic regardless of argument order")
	}
	if winnerAB != b {
		t.Error("last-write-wins: the version with the later timestamp must win")
	}
}

func TestResolveEqualClocks(t *testing.T) {
	a := &VersionedValue{Value: []byte("same"), Clock: VClock{"dc1": 1}, Timestamp: 1, NodeID: "dc1"}
	b := &VersionedValue{Value: []byte("same"), Clock: VClock{"dc1": 1}, Timestamp: 1, NodeID: "dc1"}

	if _, conflict := resolve(a, b); conflict {
		t.Error("identical versions must not be reported as a conflict")
	}

	// same causal point but different values: a genuine conflict
	c := &VersionedValue{Value: []byte("x"), Clock: VClock{}, Timestamp: 1, NodeID: "dc1"}
	d := &VersionedValue{Value: []byte("y"), Clock: VClock{}, Timestamp: 2, NodeID: "dc2"}
	winner, conflict := resolve(c, d)
	if !conflict {
		t.Error("equal clocks with different values must be reported as a conflict")
	}
	if winner != d {
		t.Error("last-write-wins: the version with the later timestamp must win")
	}
}

func TestEncodeDecodeVersioned(t *testing.T) {
	original := &VersionedValue{
		Value:     []byte("payload"),
		Clock:     VClock{"dc1": 3, "dc2": 1},
		Timestamp: 12345,
		NodeID:    "dc1",
	}

	stored, err := encodeVersioned(original)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	decoded, ok := decodeVersioned(stored)
	if !ok {
		t.Fatal("failed to decode a versioned value")
	}
	if !bytes.Equal(decoded.Value, original.Value) ||
		Compare(decoded.Clock, original.Clock) != RelationEqual ||
		decoded.Timestamp != original.Timestamp ||
		decoded.NodeID != original.NodeID {
		t.Errorf("round trip mismatch: %+v vs %+v", decoded, original)
	}
}

func TestUnwrapLegacyValue(t *testing.T) {
	// values written before versioning was introduced have no envelope
	legacy := []byte("plain-legacy-value")

	unwrapped := unwrapVersioned(legacy)
	if !bytes.Equal(unwrapped.Value, legacy) {
		t.Errorf("legacy value corrupted: %q", unwrapped.Value)
	}
	if len(unwrapped.Clock) != 0 {
		t.Errorf("legacy value must have an empty clock: %v", unwrapped.Clock)
	}
}
