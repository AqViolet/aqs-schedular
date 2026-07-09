package cron

import (
	"testing"
	"time"
)

func utc(y int, m time.Month, d, h, mi, s int) time.Time {
	return time.Date(y, m, d, h, mi, s, 0, time.UTC)
}

func TestNextAfterUTC(t *testing.T) {
	tests := []struct {
		name string
		expr string
		from time.Time
		want time.Time
	}{
		{"every 5 min mid-minute", "*/5 * * * *", utc(2026, 4, 1, 10, 2, 30), utc(2026, 4, 1, 10, 5, 0)},
		{"strictly after exact fire time", "*/5 * * * *", utc(2026, 4, 1, 10, 5, 0), utc(2026, 4, 1, 10, 10, 0)},
		{"one second before fire time", "*/5 * * * *", utc(2026, 4, 1, 10, 4, 59), utc(2026, 4, 1, 10, 5, 0)},
		{"every minute", "* * * * *", utc(2026, 4, 1, 10, 0, 0), utc(2026, 4, 1, 10, 1, 0)},
		{"minute rolls into next hour", "*/15 * * * *", utc(2026, 4, 1, 10, 45, 0), utc(2026, 4, 1, 11, 0, 0)},
		{"23:59 rolls to next day", "* * * * *", utc(2026, 4, 1, 23, 59, 10), utc(2026, 4, 2, 0, 0, 0)},
		{"hour rolls to next day", "30 2 * * *", utc(2026, 4, 1, 3, 0, 0), utc(2026, 4, 2, 2, 30, 0)},
		{"month boundary", "0 0 1 * *", utc(2026, 1, 31, 12, 0, 0), utc(2026, 2, 1, 0, 0, 0)},
		{"year boundary", "0 0 1 1 *", utc(2026, 12, 31, 23, 59, 59), utc(2027, 1, 1, 0, 0, 0)},
		{"day 31 skips short months", "0 0 31 * *", utc(2026, 1, 31, 0, 0, 0), utc(2026, 3, 31, 0, 0, 0)},
		{"day 30 skips february", "0 0 30 * *", utc(2026, 1, 30, 0, 0, 0), utc(2026, 3, 30, 0, 0, 0)},
		{"feb 29 before leap year", "0 0 29 2 *", utc(2023, 3, 1, 0, 0, 0), utc(2024, 2, 29, 0, 0, 0)},
		{"feb 29 after leap day", "0 0 29 2 *", utc(2024, 3, 1, 0, 0, 0), utc(2028, 2, 29, 0, 0, 0)},
		{"feb 29 skips 2100", "0 0 29 2 *", utc(2096, 3, 1, 0, 0, 0), utc(2104, 2, 29, 0, 0, 0)},
		{"leap day from feb 28", "0 12 * 2 *", utc(2024, 2, 28, 13, 0, 0), utc(2024, 2, 29, 12, 0, 0)},
		{"non-leap feb 28 to mar 1", "0 0 * * *", utc(2026, 2, 28, 0, 0, 0), utc(2026, 3, 1, 0, 0, 0)},

		{"dom OR dow: friday first", "0 0 13 * 5", utc(2026, 2, 1, 0, 0, 0), utc(2026, 2, 6, 0, 0, 0)},
		{"dom OR dow: 13th that is a friday", "0 0 13 * 5", utc(2026, 2, 6, 0, 0, 0), utc(2026, 2, 13, 0, 0, 0)},
		{"dom OR dow: dom hit on non-friday", "0 0 13 * 5", utc(2026, 4, 10, 12, 0, 0), utc(2026, 4, 13, 0, 0, 0)},
		{"dom OR dow: dow hit before dom", "0 0 13 * 5", utc(2026, 4, 1, 0, 0, 0), utc(2026, 4, 3, 0, 0, 0)},
		{"dom only", "0 0 13 * *", utc(2026, 4, 1, 0, 0, 0), utc(2026, 4, 13, 0, 0, 0)},
		{"dow only", "0 0 * * 1", utc(2026, 4, 1, 0, 0, 0), utc(2026, 4, 6, 0, 0, 0)},
		{"dow star step is still star (AND)", "0 0 13 * */2", utc(2026, 2, 1, 0, 0, 0), utc(2026, 6, 13, 0, 0, 0)},
		{"sunday as 0", "0 0 * * 0", utc(2026, 2, 1, 0, 0, 0), utc(2026, 2, 8, 0, 0, 0)},
		{"sunday as 7", "0 0 * * 7", utc(2026, 2, 1, 0, 0, 0), utc(2026, 2, 8, 0, 0, 0)},
		{"weekdays skip weekend", "0 9 * * 1-5", utc(2026, 4, 3, 10, 0, 0), utc(2026, 4, 6, 9, 0, 0)},
		{"names", "0 9 * JAN MON", utc(2026, 2, 1, 0, 0, 0), utc(2027, 1, 4, 9, 0, 0)},
		{"lowercase name ranges", "0 0 * * mon-wed", utc(2026, 4, 2, 0, 0, 0), utc(2026, 4, 6, 0, 0, 0)},

		{"range step first", "1-10/2 * * * *", utc(2026, 4, 1, 10, 0, 0), utc(2026, 4, 1, 10, 1, 0)},
		{"range step next", "1-10/2 * * * *", utc(2026, 4, 1, 10, 1, 0), utc(2026, 4, 1, 10, 3, 0)},
		{"range step wraps hour", "1-10/2 * * * *", utc(2026, 4, 1, 10, 9, 0), utc(2026, 4, 1, 11, 1, 0)},
		{"N/step means N-max", "5/20 * * * *", utc(2026, 4, 1, 10, 6, 0), utc(2026, 4, 1, 10, 25, 0)},
		{"list", "0,30 9,17 * * *", utc(2026, 4, 1, 9, 30, 0), utc(2026, 4, 1, 17, 0, 0)},
		{"list with range", "0 0 1,15-17 * *", utc(2026, 4, 1, 0, 0, 0), utc(2026, 4, 15, 0, 0, 0)},
		{"step over months", "0 0 1 */3 *", utc(2026, 1, 1, 0, 0, 0), utc(2026, 4, 1, 0, 0, 0)},
		{"nanoseconds ignored", "*/5 * * * *", time.Date(2026, 4, 1, 10, 4, 0, 999_999_999, time.UTC), utc(2026, 4, 1, 10, 5, 0)},

		{"@hourly", "@hourly", utc(2026, 4, 1, 10, 30, 0), utc(2026, 4, 1, 11, 0, 0)},
		{"@daily", "@daily", utc(2026, 4, 1, 10, 30, 0), utc(2026, 4, 2, 0, 0, 0)},
		{"@midnight", "@midnight", utc(2026, 4, 1, 0, 0, 0), utc(2026, 4, 2, 0, 0, 0)},
		{"@weekly", "@weekly", utc(2026, 4, 1, 10, 0, 0), utc(2026, 4, 5, 0, 0, 0)},
		{"@monthly", "@monthly", utc(2026, 4, 1, 0, 0, 0), utc(2026, 5, 1, 0, 0, 0)},
		{"@yearly", "@yearly", utc(2026, 4, 1, 0, 0, 0), utc(2027, 1, 1, 0, 0, 0)},
		{"@annually uppercase", "@ANNUALLY", utc(2026, 4, 1, 0, 0, 0), utc(2027, 1, 1, 0, 0, 0)},
		{"@every seconds", "@every 90s", utc(2026, 4, 1, 10, 0, 0), utc(2026, 4, 1, 10, 1, 30)},
		{"@every compound", "@every 1h30m", utc(2026, 4, 1, 10, 0, 0), utc(2026, 4, 1, 11, 30, 0)},
		{"extra whitespace", "  */5   *  * * *  ", utc(2026, 4, 1, 10, 2, 0), utc(2026, 4, 1, 10, 5, 0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Parse(tc.expr)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.expr, err)
			}
			if got := s.NextAfter(tc.from); !got.Equal(tc.want) {
				t.Errorf("NextAfter(%s) = %s, want %s", tc.from, got, tc.want)
			}
		})
	}
}

func TestOnce(t *testing.T) {
	s, err := Parse("once:2026-10-01T15:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(Once); !ok {
		t.Fatalf("expected Once, got %T", s)
	}
	if got := s.NextAfter(utc(2026, 9, 1, 0, 0, 0)); !got.Equal(utc(2026, 10, 1, 15, 0, 0)) {
		t.Errorf("before: got %s", got)
	}
	if got := s.NextAfter(utc(2026, 10, 1, 15, 0, 0)); !got.IsZero() {
		t.Errorf("at fire time should have no next, got %s", got)
	}
	if got := s.NextAfter(utc(2027, 1, 1, 0, 0, 0)); !got.IsZero() {
		t.Errorf("after: got %s", got)
	}
}

func TestNextAfterDST(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	at := func(y int, m time.Month, d, h, mi int) time.Time { return time.Date(y, m, d, h, mi, 0, 0, ny) }

	t.Run("spring forward skips nonexistent 02:30", func(t *testing.T) {
		s, _ := Parse("30 2 * * *")
		got := s.NextAfter(at(2026, 3, 7, 12, 0))
		if want := at(2026, 3, 9, 2, 30); !got.Equal(want) {
			t.Errorf("got %s, want %s", got, want)
		}
	})
	t.Run("spring forward every 30 min jumps the gap", func(t *testing.T) {
		s, _ := Parse("*/30 * * * *")
		from := at(2026, 3, 8, 1, 30)
		got := s.NextAfter(from)
		if want := at(2026, 3, 8, 3, 0); !got.Equal(want) {
			t.Errorf("got %s, want %s", got, want)
		}
		if got.Sub(from) != 30*time.Minute {
			t.Errorf("expected 30 real minutes, got %s", got.Sub(from))
		}
	})
	t.Run("spring forward 03:00 fires once", func(t *testing.T) {
		s, _ := Parse("0 3 * * *")
		got := s.NextAfter(at(2026, 3, 8, 0, 0))
		if want := at(2026, 3, 8, 3, 0); !got.Equal(want) {
			t.Errorf("got %s, want %s", got, want)
		}
	})
	t.Run("fall back repeated 01:30 fires on first occurrence only", func(t *testing.T) {
		s, _ := Parse("30 1 * * *")
		first := s.NextAfter(at(2026, 10, 31, 12, 0))
		if want := at(2026, 11, 1, 1, 30); !first.Equal(want) {
			t.Fatalf("first got %s, want %s", first, want)
		}
		if _, off := first.Zone(); off != -4*3600 {
			t.Fatalf("expected EDT offset, got %d", off)
		}
		second := s.NextAfter(first)
		if want := at(2026, 11, 2, 1, 30); !second.Equal(want) {
			t.Errorf("second got %s, want %s", second, want)
		}
	})
	t.Run("fall back results strictly increase", func(t *testing.T) {
		s, _ := Parse("*/30 * * * *")
		cur := at(2026, 11, 1, 0, 0)
		for i := 0; i < 12; i++ {
			next := s.NextAfter(cur)
			if !next.After(cur) {
				t.Fatalf("step %d: %s not after %s", i, next, cur)
			}
			cur = next
		}
	})
}

func TestMonotonicAndAligned(t *testing.T) {
	s, _ := Parse("*/7 * * * *")
	cur := utc(2026, 1, 1, 0, 0, 0)
	for i := 0; i < 2000; i++ {
		next := s.NextAfter(cur)
		if !next.After(cur) {
			t.Fatalf("not increasing: %s -> %s", cur, next)
		}
		if next.Minute()%7 != 0 || next.Second() != 0 {
			t.Fatalf("misaligned fire time %s", next)
		}
		cur = next
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		"", "   ",
		"* * * *", "* * * * * *",
		"60 * * * *", "-1 * * * *", "* 24 * * *", "* * 0 * *", "* * 32 * *",
		"* * * 0 *", "* * * 13 *", "* * * * 8",
		"*/0 * * * *", "*/-1 * * * *", "*/x * * * *",
		"5-1 * * * *", "1- * * * *", "-5 * * * *",
		"a * * * *", "1,,2 * * * *", "1, * * * *",
		"@bogus", "@every", "@every 0s", "@every 500ms", "@every xyz",
		"once:garbage", "once:2026-10-01",
		"0 0 31 2 *", "0 0 30 2 *", "0 0 31 4,6,9,11 *",
	}
	for _, expr := range bad {
		if s, err := Parse(expr); err == nil {
			t.Errorf("Parse(%q) = %v, want error", expr, s)
		}
	}
}

func TestParseOK(t *testing.T) {
	good := []string{
		"* * * * *", "0 0 29 2 *", "0 0 30 2 1", "0 0 ? * ?",
		"0-59 0-23 1-31 1-12 0-7", "0 0 * * sun,sat", "@every 1s", "@EVERY 2h",
	}
	for _, expr := range good {
		if _, err := Parse(expr); err != nil {
			t.Errorf("Parse(%q): %v", expr, err)
		}
	}
}
