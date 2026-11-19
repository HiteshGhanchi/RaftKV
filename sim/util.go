package sim

import (
	"cmp"
	"slices"
)

// sortedKeys returns map keys in order. Go randomises map iteration order, so
// any loop over a map that affects behaviour must go through this.
func sortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
