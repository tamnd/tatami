package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

func uuid(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

func sample(path string, b byte, tier uint8) Member {
	return Member{
		FilePath:      path,
		FileUUID:      uuid(b),
		RowCount:      1000,
		ByteSize:      50000,
		SortColumn:    "id",
		SortType:      11, // string
		SortKeyMin:    []byte("doc00000"),
		SortKeyMax:    []byte("doc09999"),
		Zones:         []ZoneBound{{Column: "status", Type: 3, Min: []byte{200, 0, 0, 0}, Max: []byte{0x94, 0x01, 0, 0}}},
		Tier:          tier,
		Crawls:        []string{"CC-MAIN-2026-22"},
		CreatedMillis: 1719360000000,
		FooterCRC:     0xdeadbeef,
	}
}

func TestMemberRoundTrip(t *testing.T) {
	in := sample("data/000001.tatami", 7, 2)
	out, err := decodeMember(encodeMember(in))
	if err != nil {
		t.Fatal(err)
	}
	if out.FilePath != in.FilePath || out.FileUUID != in.FileUUID || out.RowCount != in.RowCount {
		t.Fatalf("identity fields differ: %+v", out)
	}
	if out.SortColumn != in.SortColumn || string(out.SortKeyMin) != string(in.SortKeyMin) || string(out.SortKeyMax) != string(in.SortKeyMax) {
		t.Fatalf("sort fields differ: %+v", out)
	}
	if len(out.Zones) != 1 || out.Zones[0].Column != "status" {
		t.Fatalf("zone rollup lost: %+v", out.Zones)
	}
	if out.Tier != in.Tier || out.FooterCRC != in.FooterCRC || out.CreatedMillis != in.CreatedMillis {
		t.Fatalf("scalar fields differ: %+v", out)
	}
	if len(out.Crawls) != 1 || out.Crawls[0] != "CC-MAIN-2026-22" {
		t.Fatalf("crawls lost: %+v", out.Crawls)
	}
}

func TestReplayAddRemoveTier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tatami.manifest")
	m, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AppendAdd(sample("a.tatami", 1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := m.AppendAdd(sample("b.tatami", 2, 0)); err != nil {
		t.Fatal(err)
	}
	if err := m.AppendSetTier(uuid(2), 3); err != nil {
		t.Fatal(err)
	}
	if err := m.AppendRemove(uuid(1)); err != nil {
		t.Fatal(err)
	}

	// Re-open and replay from disk: only b remains, at tier 3.
	m2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	live := m2.Live()
	if len(live) != 1 {
		t.Fatalf("got %d live members, want 1", len(live))
	}
	if live[0].FilePath != "b.tatami" || live[0].Tier != 3 {
		t.Fatalf("unexpected live member %+v", live[0])
	}
}

func TestSwapAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tatami.manifest")
	m, _ := Open(path)
	_ = m.AppendAdd(sample("in1.tatami", 1, 0))
	_ = m.AppendAdd(sample("in2.tatami", 2, 0))
	out := sample("out.tatami", 9, 1)
	if err := m.Swap([]Member{out}, [][16]byte{uuid(1), uuid(2)}); err != nil {
		t.Fatal(err)
	}
	m2, _ := Open(path)
	live := m2.Live()
	if len(live) != 1 || live[0].FilePath != "out.tatami" {
		t.Fatalf("swap left %+v, want only out.tatami", live)
	}
}

func TestCompactBoundsLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tatami.manifest")
	m, _ := Open(path)
	for i := byte(0); i < 20; i++ {
		_ = m.AppendAdd(sample(string(rune('a'+i))+".tatami", i, 0))
	}
	for i := byte(0); i < 19; i++ { // remove all but one
		_ = m.AppendRemove(uuid(i))
	}
	before, _ := os.Stat(path)
	if err := m.Compact(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(path)
	if after.Size() >= before.Size() {
		t.Fatalf("compaction did not shrink the log: %d -> %d", before.Size(), after.Size())
	}
	m2, _ := Open(path)
	if m2.Len() != 1 {
		t.Fatalf("compacted manifest has %d members, want 1", m2.Len())
	}
}

func TestTornTailDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tatami.manifest")
	m, _ := Open(path)
	_ = m.AppendAdd(sample("a.tatami", 1, 0))
	_ = m.AppendAdd(sample("b.tatami", 2, 0))

	// Append garbage tail bytes, simulating a torn write.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{EditAdd, 0xff, 0xff, 0x01, 0x02})
	_ = f.Close()

	m2, err := Open(path)
	if err != nil {
		t.Fatalf("replay should recover the intact prefix, got %v", err)
	}
	if m2.Len() != 2 {
		t.Fatalf("torn tail corrupted the live set: %d members, want 2", m2.Len())
	}
}

func TestByteStable(t *testing.T) {
	a := encodeMember(sample("x.tatami", 5, 1))
	b := encodeMember(sample("x.tatami", 5, 1))
	if string(a) != string(b) {
		t.Fatal("member encoding is not deterministic")
	}
}
