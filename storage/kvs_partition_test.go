package storage

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"

	"go.uber.org/zap"
)

func newTestKVS(t *testing.T, nodeID string) *KVS {
	t.Helper()

	dir := t.TempDir()

	kvs, err := NewKVS(dir, dir, nodeID, zap.NewNop())
	if err != nil {
		t.Fatalf("failed to create KVS: %v", err)
	}
	t.Cleanup(func() {
		if err := kvs.Close(); err != nil {
			t.Fatalf("failed to close KVS: %v", err)
		}
	})

	return kvs
}

func reconcileOneWay(t *testing.T, from *KVS, to *KVS) []*Conflict {
	t.Helper()
	return to.Reconcile(from.VersionedItems())
}

func conflictKeys(conflicts ...[]*Conflict) []string {
	keySet := map[string]struct{}{}
	for _, report := range conflicts {
		for _, c := range report {
			keySet[c.Key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func allKeys(t *testing.T, kvs *KVS) []string {
	t.Helper()

	keys := make([]string, 0)
	for item := range kvs.VersionedItems() {
		if item == nil {
			continue
		}
		keys = append(keys, item.Key)
	}
	sort.Strings(keys)
	return keys
}

func assertConverged(t *testing.T, a *KVS, b *KVS) {
	t.Helper()

	keysA := allKeys(t, a)
	keysB := allKeys(t, b)
	if !reflect.DeepEqual(keysA, keysB) {
		t.Fatalf("replicas hold different keys: %v vs %v", keysA, keysB)
	}

	for _, key := range keysA {
		va, err := a.GetVersioned(key)
		if err != nil {
			t.Fatalf("failed to get %q from replica a: %v", key, err)
		}
		vb, err := b.GetVersioned(key)
		if err != nil {
			t.Fatalf("failed to get %q from replica b: %v", key, err)
		}
		if !reflect.DeepEqual(va, vb) {
			t.Errorf("replicas diverged on key %q: %+v vs %+v", key, va, vb)
		}
	}
}

// TestPartitionHealConverges simulates the incident: the link between the two
// datacenters breaks, both sides keep writing, and after the link is restored
// the replicas must reconcile automatically. Only keys genuinely written on
// both sides during the partition may be reported as conflicts.
func TestPartitionHealConverges(t *testing.T) {
	dc1 := newTestKVS(t, "dc1")
	dc2 := newTestKVS(t, "dc2")

	// before the partition: dc1 takes writes, replication carries them to dc2
	prePartition := map[string]string{
		"shared/conflict": "base",
		"shared/seq":      "s0",
		"shared/stable":   "stable",
	}
	for key, value := range prePartition {
		if err := dc1.Set(key, []byte(value)); err != nil {
			t.Fatalf("failed to set %q: %v", key, err)
		}
	}
	if conflicts := reconcileOneWay(t, dc1, dc2); len(conflicts) != 0 {
		t.Fatalf("replicating to an empty replica reported conflicts: %+v", conflicts)
	}

	// the link breaks: both sides keep accepting writes independently

	// written on both sides during the partition: the genuine conflict
	if err := dc1.Set("shared/conflict", []byte("dc1-version")); err != nil {
		t.Fatal(err)
	}
	if err := dc2.Set("shared/conflict", []byte("dc2-version")); err != nil {
		t.Fatal(err)
	}

	// written on one side only: must converge without a conflict
	if err := dc1.Set("only/dc1", []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := dc2.Set("only/dc2", []byte("b1")); err != nil {
		t.Fatal(err)
	}

	// written several times on one side only: a normal causal chain,
	// not a conflict
	for _, value := range []string{"s1", "s2", "s3"} {
		if err := dc1.Set("shared/seq", []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	for _, value := range []string{"x1", "x2"} {
		if err := dc2.Set("shared/dc2-seq", []byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	// the link is restored: reconcile in both directions
	conflictsDC2 := reconcileOneWay(t, dc1, dc2)
	conflictsDC1 := reconcileOneWay(t, dc2, dc1)

	// exactly the keys written on both sides during the partition are conflicts
	expectedConflicts := []string{"shared/conflict"}
	if keys := conflictKeys(conflictsDC2, conflictsDC1); !reflect.DeepEqual(keys, expectedConflicts) {
		t.Errorf("conflict keys = %v, expected %v", keys, expectedConflicts)
	}

	// both replicas converge to identical state, versions included
	assertConverged(t, dc1, dc2)

	// one-sided writes survived on both replicas
	for key, expected := range map[string]string{
		"only/dc1":       "a1",
		"only/dc2":       "b1",
		"shared/seq":     "s3",
		"shared/dc2-seq": "x2",
		"shared/stable":  "stable",
	} {
		value, err := dc1.Get(key)
		if err != nil {
			t.Fatalf("failed to get %q: %v", key, err)
		}
		if string(value) != expected {
			t.Errorf("key %q = %q, expected %q", key, value, expected)
		}
	}

	// the conflict was resolved deterministically: both replicas picked the
	// same winner, and the conflict report preserves both versions for audit
	final, err := dc1.Get("shared/conflict")
	if err != nil {
		t.Fatal(err)
	}
	if string(final) != "dc1-version" && string(final) != "dc2-version" {
		t.Errorf("unexpected winner for shared/conflict: %q", final)
	}
	if len(conflictsDC2) != 1 {
		t.Fatalf("expected exactly one conflict on dc2, got %+v", conflictsDC2)
	}
	report := conflictsDC2[0]
	if string(report.LocalValue) != "dc2-version" || string(report.RemoteValue) != "dc1-version" {
		t.Errorf("conflict report lost a version: %+v", report)
	}
	if string(report.WinnerValue) != string(final) {
		t.Errorf("conflict report winner %q does not match final value %q", report.WinnerValue, final)
	}

	// reconciling again is a no-op: converged replicas report nothing
	if keys := conflictKeys(reconcileOneWay(t, dc1, dc2), reconcileOneWay(t, dc2, dc1)); len(keys) != 0 {
		t.Errorf("reconciliation is not idempotent, unexpected conflicts: %v", keys)
	}
	assertConverged(t, dc1, dc2)
}

// TestNormalConcurrentWritesNoConflict ensures that the normal write path -
// many clients writing through a single primary, where the Raft log
// serializes all writes - never produces conflicts, neither during
// replication nor in the final reconciliation.
func TestNormalConcurrentWritesNoConflict(t *testing.T) {
	primary := newTestKVS(t, "dc1")
	replica := newTestKVS(t, "dc2")

	// many clients writing concurrently to the primary; the storage
	// serializes their writes into a single causal chain
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				key := fmt.Sprintf("concurrent/key-%d", (worker*25+i)%50)
				if err := primary.Set(key, []byte(fmt.Sprintf("w%d-i%d", worker, i))); err != nil {
					t.Errorf("failed to set %q: %v", key, err)
				}
			}
		}(worker)
	}
	wg.Wait()

	// replication and the final reconciliation must report zero conflicts
	if keys := conflictKeys(reconcileOneWay(t, primary, replica)); len(keys) != 0 {
		t.Errorf("normal concurrent writes reported as conflicts: %v", keys)
	}
	if keys := conflictKeys(reconcileOneWay(t, replica, primary)); len(keys) != 0 {
		t.Errorf("normal concurrent writes reported as conflicts: %v", keys)
	}

	assertConverged(t, primary, replica)
}

// TestLegacyValuesDominated ensures data written before versioning was
// introduced reconciles cleanly: it is treated as causally oldest and never
// triggers a conflict against versioned writes.
func TestLegacyValuesDominated(t *testing.T) {
	legacy := newTestKVS(t, "dc1")
	current := newTestKVS(t, "dc2")

	if err := legacy.SetRaw("legacy/key", []byte("plain-old-value")); err != nil {
		t.Fatal(err)
	}
	if err := current.Set("legacy/key", []byte("new-value")); err != nil {
		t.Fatal(err)
	}

	// the legacy store must still serve its raw value
	value, err := legacy.Get("legacy/key")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "plain-old-value" {
		t.Errorf("legacy value corrupted: %q", value)
	}

	conflicts := reconcileOneWay(t, current, legacy)
	if len(conflicts) != 0 {
		t.Errorf("legacy value conflicted with a versioned write: %+v", conflicts)
	}
	value, err = legacy.Get("legacy/key")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "new-value" {
		t.Errorf("versioned write must dominate the legacy value, got %q", value)
	}
}

// TestSnapshotPreservesVersions ensures Raft snapshots carry the causal
// history, so a node restored from a snapshot reconciles correctly.
func TestSnapshotPreservesVersions(t *testing.T) {
	source := newTestKVS(t, "dc1")
	restored := newTestKVS(t, "dc2")

	if err := source.Set("snap/key", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := source.Set("snap/key", []byte("v2")); err != nil {
		t.Fatal(err)
	}

	ch := source.SnapshotItems()
	for {
		kvp := <-ch
		if kvp == nil {
			break
		}
		if err := restored.SetRaw(kvp.Key, kvp.Value); err != nil {
			t.Fatal(err)
		}
	}

	original, err := source.GetVersioned("snap/key")
	if err != nil {
		t.Fatal(err)
	}
	clone, err := restored.GetVersioned("snap/key")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, clone) {
		t.Errorf("snapshot lost the version: %+v vs %+v", original, clone)
	}

	// a replica restored from a snapshot reconciles without conflicts
	if keys := conflictKeys(reconcileOneWay(t, source, restored), reconcileOneWay(t, restored, source)); len(keys) != 0 {
		t.Errorf("restored replica reported conflicts: %v", keys)
	}
}
