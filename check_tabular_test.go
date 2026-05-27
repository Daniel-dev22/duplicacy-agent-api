package main

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"
)

// TestParsePrettyBytes pins the binary-suffix conversion. Wrong here means
// the storage-rollup chart shows nonsense.
func TestParsePrettyBytes(t *testing.T) {
	k := func(f float64, shift uint) int64 { return int64(f * float64(int64(1)<<shift)) }
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"-", 0},
		{"0", 0},
		{"512", 512},
		{"1.2K", k(1.2, 10)},
		{"145.2G", k(145.2, 30)},
		{"8,353M", 8353 * (1 << 20)},
		{"1T", 1 << 40},
		{"2P", 2 << 50},
	}
	for _, c := range cases {
		got := parsePrettyBytes(c.in)
		if got != c.want {
			t.Errorf("parsePrettyBytes(%q) = %d want %d", c.in, got, c.want)
		}
	}
}

// TestCheckTabularParser feeds the fixture through line-by-line and
// verifies (1) header enters table mode, (2) three per-revision rows are
// emitted, (3) the "all" rollup row is skipped, (4) non-table lines before
// and after are ignored.
func TestCheckTabularParser(t *testing.T) {
	f, err := os.Open(filepath.Join("testdata", "check_tabular.txt"))
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer f.Close()

	p := &checkTabularParser{}
	var rows []*snapshotStatRow
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if r := p.feed(sc.Text()); r != nil {
			rows = append(rows, r)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3 (rollup row should be skipped)", len(rows))
	}

	// Spot-check revision 1: 100 chunks, 400M total/uniq/new bytes (first
	// revision has full uniqueness against itself).
	r1 := rows[0]
	if r1.SnapshotID != "host01" || r1.Revision != 1 {
		t.Errorf("row 0 id/rev = %s/%d want host01/1", r1.SnapshotID, r1.Revision)
	}
	if r1.Files != 4440 {
		t.Errorf("row 0 files = %d want 4440", r1.Files)
	}
	wantTotal := int64(400 * (1 << 20))
	if r1.TotalBytes != wantTotal || r1.UniqBytes != wantTotal || r1.NewBytes != wantTotal {
		t.Errorf("row 0 bytes (total/uniq/new) = %d/%d/%d, all want %d",
			r1.TotalBytes, r1.UniqBytes, r1.NewBytes, wantTotal)
	}
	if r1.TotalChunks != 100 || r1.UniqChunks != 100 || r1.NewChunks != 100 {
		t.Errorf("row 0 chunks (total/uniq/new) = %d/%d/%d, all want 100",
			r1.TotalChunks, r1.UniqChunks, r1.NewChunks)
	}
	if r1.TotalBytesPretty != "400.0M" {
		t.Errorf("row 0 total_bytes_pretty = %q want 400.0M", r1.TotalBytesPretty)
	}

	// Revision 2 is an incremental: 2 new chunks of 8M.
	r2 := rows[1]
	if r2.NewChunks != 2 || r2.NewBytes != int64(8*(1<<20)) {
		t.Errorf("row 1 new chunks/bytes = %d/%d, want 2/%d", r2.NewChunks, r2.NewBytes, 8*(1<<20))
	}

	// Revision 3 is also incremental: 1 new chunk of 4M.
	r3 := rows[2]
	if r3.NewChunks != 1 || r3.NewBytes != int64(4*(1<<20)) {
		t.Errorf("row 2 new chunks/bytes = %d/%d, want 1/%d", r3.NewChunks, r3.NewBytes, 4*(1<<20))
	}
}

// TestCheckTabularParser_TableExit verifies that a non-pipe line after the
// table returns the parser to "out of table" mode, so subsequent log lines
// don't accidentally re-parse as rows.
func TestCheckTabularParser_TableExit(t *testing.T) {
	p := &checkTabularParser{}
	lines := []string{
		"  snap |   rev |   col3 |   files |   bytes |   chunks |   bytes |   uniq |   bytes |   new |   bytes |",
		"foo | 1 | @ts | 1 | 1K | 1 | 1K | 1 | 1K | 1 | 1K |",
		"INFO SNAPSHOT_CHECK All chunks referenced by snapshot foo at revision 1 exist",
		// This is the SNAPSHOT_CHECK summary, NOT a tabular row. The previous
		// non-pipe line must have already flipped the parser out of table mode.
		"INFO SNAPSHOT_CHECK Snapshot foo all revisions: 1K total chunk bytes, 1K unique chunk bytes",
	}
	var emitted int
	for _, l := range lines {
		if r := p.feed(l); r != nil {
			emitted++
		}
	}
	if emitted != 1 {
		t.Fatalf("emitted %d rows, want exactly 1 (only the in-table row)", emitted)
	}
	if p.inTable {
		t.Errorf("parser should be out of table after non-pipe lines")
	}
}
