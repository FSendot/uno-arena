package main

import (
	"testing"
	"time"
)

func TestJitterDurationIsDeterministicAndBounded(t *testing.T) {
	base := 10 * time.Second
	for _, tc := range []struct {
		sample float64
		want   time.Duration
	}{{-1, 8 * time.Second}, {0.5, 10 * time.Second}, {2, 12 * time.Second}} {
		if got := jitterDuration(base, tc.sample); got != tc.want {
			t.Fatalf("sample=%v got=%s want=%s", tc.sample, got, tc.want)
		}
	}
}
