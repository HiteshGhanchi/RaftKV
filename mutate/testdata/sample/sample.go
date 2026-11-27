package sample

type T struct {
	n   int
	mut flags
}

type flags uint

func (f flags) Has(x flags) bool { return f&x != 0 }

func (t *T) String() string {
	if t.n > 0 {
		return "positive"
	}
	return "other"
}

func Step(t *T, a, b int) int {
	if t.mut.Has(1) {
		return -1
	}
	if a < b && b != 0 {
		t.n++
	}
	t.n = a + 1
	switch {
	case a >= b:
		t.n -= 2
	}
	return t.n
}
