package main

import (
	"testing"
	"time"
)

func TestFireJitter(t *testing.T) {
	cap := 60 * time.Second
	// Deterministic: same input → same output
	a := fireJitter("kd-pi", "abc-123", cap)
	b := fireJitter("kd-pi", "abc-123", cap)
	if a != b {
		t.Fatalf("expected deterministic jitter, got %v vs %v", a, b)
	}
	// Bounded: 0 <= jitter < cap
	if a < 0 || a >= cap {
		t.Fatalf("jitter %v out of [0, %v)", a, cap)
	}
	// Different inputs spread across the window — no two of N=20 distinct
	// (node, schedule) pairs collapse to the same nanosecond.
	seen := make(map[time.Duration]string)
	for _, n := range []string{"kd-nuc", "kd-pi", "kd-nas", "ng-nuc", "ng-pi"} {
		for _, s := range []string{"sched-1", "sched-2", "sched-3", "sched-4"} {
			j := fireJitter(n, s, cap)
			if prev, ok := seen[j]; ok {
				t.Fatalf("jitter collision %v: %q vs %q (acceptable with 60s cap × 20 inputs, but worth flagging)",
					j, prev, n+"/"+s)
			}
			seen[j] = n + "/" + s
		}
	}
}

func TestFireJitterZeroCap(t *testing.T) {
	if d := fireJitter("any", "any", 0); d != 0 {
		t.Fatalf("expected 0 jitter when cap=0, got %v", d)
	}
}
