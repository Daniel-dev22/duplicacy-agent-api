package main

import (
	"regexp"
	"strconv"
	"strings"
)

// snapshotStatRow is one parsed per-revision row from `duplicacy check
// -tabular` output. It's the agent-internal shape collected during a check
// job and flushed to the snapshot_stats SQLite table on terminal state.
type snapshotStatRow struct {
	SnapshotID       string
	Revision         int
	Files            int
	Bytes            int64
	BytesPretty      string
	TotalChunks      int
	TotalBytes       int64
	TotalBytesPretty string
	UniqChunks       int
	UniqBytes        int64
	UniqBytesPretty  string
	NewChunks        int
	NewBytes         int64
	NewBytesPretty   string
}

// checkTabularParser is the stateful per-job collector for the table emitted
// by `duplicacy check -tabular`. The agent feeds it every stdout line; the
// parser ignores non-table lines, enters "in table" mode on the header, and
// returns a row per per-revision data line. The "all"-revision rollup row is
// skipped (we don't need it — the rollup can be computed from the per-rev
// rows on the query side).
//
// Format reference (duplicacy_snapshotmanager.go lines 1186-1265): the table
// is written with tabwriter.AlignRight|tabwriter.Debug, so cells are joined
// by '|' and right-padded. Header:
//
//	snap | rev |                  | files | bytes | chunks | bytes | uniq | bytes | new | bytes |
//
// Per-revision row:
//
//	<snap-id> | <rev> | @ <YYYY-MM-DD HH:MM> <opts> | <files> | <PrettyBytes> | <total-chunks> | <PrettyBytes> | <uniq-chunks> | <PrettyBytes> | <new-chunks> | <PrettyBytes> |
type checkTabularParser struct {
	inTable bool
}

// checkTabularHeaderRe matches the table header line. We use a loose anchor
// ("snap" + "rev" + "files" + at least one "bytes" between pipes) because
// tabwriter inserts variable whitespace inside each cell.
var checkTabularHeaderRe = regexp.MustCompile(
	`\bsnap\s*\|\s*rev\s*\|.*\bfiles\s*\|\s*bytes\s*\|`,
)

// feed pushes one stdout line into the parser. Returns a non-nil row when
// the line was a per-revision data row (the row is fully populated and
// caller-owned); returns nil otherwise. The "all"-revision rollup row is
// recognised and silently skipped.
func (p *checkTabularParser) feed(line string) *snapshotStatRow {
	if checkTabularHeaderRe.MatchString(line) {
		p.inTable = true
		return nil
	}
	if !p.inTable {
		return nil
	}
	// A blank line or any non-pipe line ends the table. (duplicacy prints the
	// rollup rows immediately after the per-rev rows, then exits the table.)
	if !strings.Contains(line, "|") {
		p.inTable = false
		return nil
	}

	cells := splitTabularRow(line)
	// A valid per-revision row has 11 cells: snap, rev, "@ ts opts", files,
	// bytes, total-chunks, total-bytes, uniq-chunks, uniq-bytes, new-chunks,
	// new-bytes. The rollup row has the same 11 cells but with "all" in the
	// rev column and empties in the chronological/file columns.
	if len(cells) != 11 {
		return nil
	}
	if cells[1] == "all" || cells[1] == "" {
		return nil // per-snapshot rollup — skip
	}
	rev, err := strconv.Atoi(cells[1])
	if err != nil {
		return nil
	}

	row := &snapshotStatRow{
		SnapshotID:       cells[0],
		Revision:         rev,
		Files:            parseIntCell(cells[3]),
		BytesPretty:      cells[4],
		Bytes:            parsePrettyBytes(cells[4]),
		TotalChunks:      parseIntCell(cells[5]),
		TotalBytesPretty: cells[6],
		TotalBytes:       parsePrettyBytes(cells[6]),
		UniqChunks:       parseIntCell(cells[7]),
		UniqBytesPretty:  cells[8],
		UniqBytes:        parsePrettyBytes(cells[8]),
		NewChunks:        parseIntCell(cells[9]),
		NewBytesPretty:   cells[10],
		NewBytes:         parsePrettyBytes(cells[10]),
	}
	return row
}

// splitTabularRow splits a tabwriter.Debug row on '|', trims whitespace from
// each cell, and drops the trailing empty cell that comes from the closing
// pipe at end-of-line.
func splitTabularRow(line string) []string {
	parts := strings.Split(line, "|")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	// tabwriter ends each row with "|", producing an empty trailing cell.
	if len(out) > 0 && out[len(out)-1] == "" {
		out = out[:len(out)-1]
	}
	return out
}

// parseIntCell parses a cell that should be a bare integer. Returns 0 on
// failure — the row is still emitted so a single weird cell doesn't drop the
// whole row.
func parseIntCell(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

// parsePrettyBytes converts duplicacy's PrettyNumber output (e.g. "1.2K",
// "145.2G", "8,353M", or a bare "1024") back into raw bytes. Suffixes are
// binary (1024-based), matching duplicacy's PrettySize in
// duplicacy_chunkoperator.go.
//
// Returns 0 on parse failure — the caller still records the pretty string,
// so the UI can render the human form even if the numeric column is wrong.
func parsePrettyBytes(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return 0
	}
	// Duplicacy uses comma as a thousands separator in some pretty forms
	// (e.g. "8,353M"). Strip them before suffix processing.
	s = strings.ReplaceAll(s, ",", "")

	var mult int64 = 1
	if len(s) > 0 {
		switch s[len(s)-1] {
		case 'K', 'k':
			mult = 1 << 10
			s = s[:len(s)-1]
		case 'M', 'm':
			mult = 1 << 20
			s = s[:len(s)-1]
		case 'G', 'g':
			mult = 1 << 30
			s = s[:len(s)-1]
		case 'T', 't':
			mult = 1 << 40
			s = s[:len(s)-1]
		case 'P', 'p':
			mult = 1 << 50
			s = s[:len(s)-1]
		}
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(v * float64(mult))
	}
	return 0
}
