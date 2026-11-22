//go:build race

package sim

// raceEnabled is true when the tests are built with -race. The race detector
// slows execution several times over, so wall-clock speed checks are skipped.
const raceEnabled = true
