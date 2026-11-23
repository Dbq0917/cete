package storage

import "testing"

func TestCompare(t *testing.T) {
	testCases := []struct {
		name     string
		a        VClock
		b        VClock
		expected Relation
	}{
		{
			name:     "both empty",
			a:        VClock{},
			b:        VClock{},
			expected: RelationEqual,
		},
		{
			name:     "identical",
			a:        VClock{"dc1": 2, "dc2": 1},
			b:        VClock{"dc1": 2, "dc2": 1},
			expected: RelationEqual,
		},
		{
			name:     "a dominates b",
			a:        VClock{"dc1": 2, "dc2": 1},
			b:        VClock{"dc1": 1, "dc2": 1},
			expected: RelationDominates,
		},
		{
			name:     "a dominated by b",
			a:        VClock{"dc1": 1},
			b:        VClock{"dc1": 1, "dc2": 3},
			expected: RelationDominatedBy,
		},
		{
			name:     "empty is dominated",
			a:        VClock{},
			b:        VClock{"dc1": 1},
			expected: RelationDominatedBy,
		},
		{
			name:     "concurrent writes on both sides of a partition",
			a:        VClock{"dc1": 2},
			b:        VClock{"dc1": 1, "dc2": 1},
			expected: RelationConcurrent,
		},
		{
			name:     "concurrent disjoint clocks",
			a:        VClock{"dc1": 1},
			b:        VClock{"dc2": 1},
			expected: RelationConcurrent,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if rel := Compare(tc.a, tc.b); rel != tc.expected {
				t.Errorf("Compare(%v, %v) = %s, expected %s", tc.a, tc.b, rel, tc.expected)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	a := VClock{"dc1": 2, "dc2": 1}
	b := VClock{"dc1": 1, "dc3": 5}

	merged := Merge(a, b)

	expected := VClock{"dc1": 2, "dc2": 1, "dc3": 5}
	if Compare(merged, expected) != RelationEqual || len(merged) != len(expected) {
		t.Errorf("Merge(%v, %v) = %v, expected %v", a, b, merged, expected)
	}

	if _, ok := a["dc3"]; ok {
		t.Errorf("Merge mutated its input: %v", a)
	}
}

func TestIncrement(t *testing.T) {
	clock := VClock{}
	clock.Increment("dc1")
	clock.Increment("dc1")
	clock.Increment("dc2")

	if clock["dc1"] != 2 || clock["dc2"] != 1 {
		t.Errorf("unexpected clock after increments: %v", clock)
	}
}
