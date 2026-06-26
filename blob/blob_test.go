package blob

import (
	"bytes"
	"fmt"
	"testing"
)

func sampleValues(n int) [][]byte {
	v := make([][]byte, n)
	for i := 0; i < n; i++ {
		v[i] = []byte(fmt.Sprintf("value-%d-payload", i))
	}
	return v
}

func TestPackUnpackRoundTrip(t *testing.T) {
	cases := [][][]byte{
		nil,
		{},
		{[]byte("")},
		{[]byte("a")},
		{[]byte("a"), nil, []byte("ccc")},
		sampleValues(50),
	}
	for ci, in := range cases {
		payload := PackRun(in)
		out, err := UnpackRun(payload)
		if err != nil {
			t.Fatalf("case %d: unpack: %v", ci, err)
		}
		if len(out) != len(in) {
			t.Fatalf("case %d: got %d values want %d", ci, len(out), len(in))
		}
		for i := range in {
			if !bytes.Equal(out[i], in[i]) {
				t.Fatalf("case %d value %d: got %q want %q", ci, i, out[i], in[i])
			}
		}
	}
}

func TestPlanRuns(t *testing.T) {
	// Each value is 10 bytes; a 25-byte target should hold two per run.
	vals := make([][]byte, 5)
	for i := range vals {
		vals[i] = bytes.Repeat([]byte("x"), 10)
	}
	runs := PlanRuns(vals, 25)
	got := 0
	for _, c := range runs {
		if c < 1 {
			t.Fatalf("empty run in plan %v", runs)
		}
		got += c
	}
	if got != len(vals) {
		t.Fatalf("plan covers %d values, want %d", got, len(vals))
	}
	// An oversized single value still forms its own run.
	big := [][]byte{bytes.Repeat([]byte("y"), 100)}
	if r := PlanRuns(big, 10); len(r) != 1 || r[0] != 1 {
		t.Fatalf("oversized value plan: %v", r)
	}
	if r := PlanRuns(nil, 10); r != nil {
		t.Fatalf("empty plan: %v", r)
	}
}

func TestSampleDict(t *testing.T) {
	if d := SampleDict(nil, 1024); d != nil {
		t.Fatalf("empty input dict: %v", d)
	}
	vals := sampleValues(1000)
	d := SampleDict(vals, 512)
	if len(d) == 0 || len(d) > 512 {
		t.Fatalf("dict length %d out of bounds", len(d))
	}
	// Deterministic for the same input.
	if !bytes.Equal(d, SampleDict(vals, 512)) {
		t.Fatal("SampleDict not deterministic")
	}
}

func TestDirectoryLocate(t *testing.T) {
	d := NewDirectory([]int{3, 1, 4})
	if d.Len() != 8 {
		t.Fatalf("len: got %d want 8", d.Len())
	}
	want := []struct{ run, idx int }{
		{0, 0}, {0, 1}, {0, 2}, // run 0 holds ordinals 0..2
		{1, 0},                         // run 1 holds ordinal 3
		{2, 0}, {2, 1}, {2, 2}, {2, 3}, // run 2 holds ordinals 4..7
	}
	for ord, w := range want {
		run, idx, err := d.Locate(ord)
		if err != nil {
			t.Fatalf("ordinal %d: %v", ord, err)
		}
		if run != w.run || idx != w.idx {
			t.Fatalf("ordinal %d: got (run %d, idx %d) want (run %d, idx %d)", ord, run, idx, w.run, w.idx)
		}
	}
	if _, _, err := d.Locate(8); err == nil {
		t.Fatal("expected out-of-range error for ordinal 8")
	}
	if _, _, err := d.Locate(-1); err == nil {
		t.Fatal("expected out-of-range error for ordinal -1")
	}
}
