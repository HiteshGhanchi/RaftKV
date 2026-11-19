package mutation

import "testing"

func TestParseRoundTrip(t *testing.T) {
	var all Set
	for _, m := range All {
		all |= m.Flag
	}
	got, err := Parse(all.String())
	if err != nil || got != all {
		t.Fatalf("Parse(%q) = %v, %v", all.String(), got, err)
	}
	if s, _ := Parse("none"); s != 0 {
		t.Fatal("none should be empty")
	}
	if _, err := Parse("not-a-bug"); err == nil {
		t.Fatal("unknown name accepted")
	}
}
