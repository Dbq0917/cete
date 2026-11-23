package storage

import (
	"sort"
	"strings"
)

// Relation describes the causal relationship between two vector clocks.
type Relation int

const (
	// RelationEqual means both clocks record exactly the same causal history.
	RelationEqual Relation = iota
	// RelationDominates means the left clock strictly happens after the right one.
	RelationDominates
	// RelationDominatedBy means the left clock strictly happens before the right one.
	RelationDominatedBy
	// RelationConcurrent means neither clock happens before the other: the two
	// versions were written without knowing of each other (e.g. on both sides of
	// a network partition). Only this relation is a genuine conflict.
	RelationConcurrent
)

func (r Relation) String() string {
	switch r {
	case RelationEqual:
		return "equal"
	case RelationDominates:
		return "dominates"
	case RelationDominatedBy:
		return "dominated-by"
	case RelationConcurrent:
		return "concurrent"
	default:
		return "unknown"
	}
}

// VClock is a version vector: node ID -> logical counter of the last write
// performed by that node (transitively observed by the holder of the clock).
type VClock map[string]uint64

// Copy returns a deep copy of the clock.
func (c VClock) Copy() VClock {
	copied := make(VClock, len(c))
	for nodeID, counter := range c {
		copied[nodeID] = counter
	}
	return copied
}

// Increment advances the counter of the given node and returns the clock.
func (c VClock) Increment(nodeID string) VClock {
	c[nodeID] = c[nodeID] + 1
	return c
}

// Merge returns a new clock holding the element-wise maximum of both clocks.
func Merge(a VClock, b VClock) VClock {
	merged := a.Copy()
	for nodeID, counter := range b {
		if counter > merged[nodeID] {
			merged[nodeID] = counter
		}
	}
	return merged
}

// Compare determines the causal relationship between two clocks.
func Compare(a VClock, b VClock) Relation {
	aGreater := false
	bGreater := false

	for nodeID, aCounter := range a {
		bCounter := b[nodeID]
		if aCounter > bCounter {
			aGreater = true
		} else if bCounter > aCounter {
			bGreater = true
		}
	}
	for nodeID, bCounter := range b {
		if _, ok := a[nodeID]; !ok && bCounter > 0 {
			bGreater = true
		}
	}

	switch {
	case aGreater && bGreater:
		return RelationConcurrent
	case aGreater:
		return RelationDominates
	case bGreater:
		return RelationDominatedBy
	default:
		return RelationEqual
	}
}

// String returns a deterministic, human readable representation of the clock.
func (c VClock) String() string {
	nodeIDs := make([]string, 0, len(c))
	for nodeID := range c {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	parts := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		parts = append(parts, nodeID+"="+itoa(c[nodeID]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v = v / 10
	}
	return string(buf[i:])
}
