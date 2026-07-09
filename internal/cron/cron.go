package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Schedule interface {
	NextAfter(t time.Time) time.Time
}

type Every time.Duration

func (e Every) NextAfter(t time.Time) time.Time { return t.Add(time.Duration(e)) }

type Once time.Time

func (o Once) NextAfter(t time.Time) time.Time {
	if at := time.Time(o); at.After(t) {
		return at
	}
	return time.Time{}
}

func (o Once) At() time.Time { return time.Time(o) }

type Spec struct {
	minute, hour, dom, month, dow uint64
	domStar, dowStar              bool
}

var macros = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dowNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

func Parse(expr string) (Schedule, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("cron: empty expression")
	}

	if rest, ok := strings.CutPrefix(expr, "once:"); ok {
		at, err := time.Parse(time.RFC3339, strings.TrimSpace(rest))
		if err != nil {
			return nil, fmt.Errorf("cron: invalid once timestamp: %w", err)
		}
		return Once(at), nil
	}

	if strings.HasPrefix(expr, "@") {
		if len(expr) >= 6 && strings.EqualFold(expr[:6], "@every") {
			d, err := time.ParseDuration(strings.TrimSpace(expr[6:]))
			if err != nil {
				return nil, fmt.Errorf("cron: invalid @every duration: %w", err)
			}
			if d < time.Second {
				return nil, fmt.Errorf("cron: @every duration must be at least 1s, got %s", d)
			}
			return Every(d), nil
		}
		m, ok := macros[strings.ToLower(expr)]
		if !ok {
			return nil, fmt.Errorf("cron: unknown macro %q", expr)
		}
		expr = m
	}

	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields, got %d in %q", len(fields), expr)
	}

	var s Spec
	var err error
	if s.minute, _, err = parseField(fields[0], 0, 59, nil); err != nil {
		return nil, fmt.Errorf("cron: minute: %w", err)
	}
	if s.hour, _, err = parseField(fields[1], 0, 23, nil); err != nil {
		return nil, fmt.Errorf("cron: hour: %w", err)
	}
	if s.dom, s.domStar, err = parseField(fields[2], 1, 31, nil); err != nil {
		return nil, fmt.Errorf("cron: day-of-month: %w", err)
	}
	if s.month, _, err = parseField(fields[3], 1, 12, monthNames); err != nil {
		return nil, fmt.Errorf("cron: month: %w", err)
	}
	if s.dow, s.dowStar, err = parseField(fields[4], 0, 7, dowNames); err != nil {
		return nil, fmt.Errorf("cron: day-of-week: %w", err)
	}
	if s.dow&(1<<7) != 0 {
		s.dow = s.dow&^(1<<7) | 1
	}
	if !s.domStar && s.dowStar && !s.canMatchDayOfMonth() {
		return nil, fmt.Errorf("cron: day-of-month/month combination never occurs")
	}
	return &s, nil
}

func (s *Spec) canMatchDayOfMonth() bool {
	maxDays := [13]int{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	for m := 1; m <= 12; m++ {
		if s.month&(1<<uint(m)) == 0 {
			continue
		}
		for d := 1; d <= maxDays[m]; d++ {
			if s.dom&(1<<uint(d)) != 0 {
				return true
			}
		}
	}
	return false
}

func parseField(field string, lo, hi int, names map[string]int) (mask uint64, star bool, err error) {
	if field == "" {
		return 0, false, fmt.Errorf("empty field")
	}
	star = field[0] == '*' || field[0] == '?'
	for _, part := range strings.Split(field, ",") {
		if part == "" {
			return 0, false, fmt.Errorf("empty list element in %q", field)
		}
		expr, stepStr, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			step, err = strconv.Atoi(stepStr)
			if err != nil || step <= 0 {
				return 0, false, fmt.Errorf("invalid step %q", stepStr)
			}
		}

		var start, end int
		switch {
		case expr == "*" || expr == "?":
			start, end = lo, hi
			if lo == 0 && hi == 7 {
				end = 6
			}
		case strings.Contains(expr, "-"):
			a, b, _ := strings.Cut(expr, "-")
			if start, err = parseValue(a, lo, hi, names); err != nil {
				return 0, false, err
			}
			if end, err = parseValue(b, lo, hi, names); err != nil {
				return 0, false, err
			}
			if start > end {
				return 0, false, fmt.Errorf("range %q is reversed", expr)
			}
		default:
			if start, err = parseValue(expr, lo, hi, names); err != nil {
				return 0, false, err
			}
			end = start
			if hasStep {
				end = hi
			}
		}
		for v := start; v <= end; v += step {
			mask |= 1 << uint(v)
		}
	}
	return mask, star, nil
}

func parseValue(s string, lo, hi int, names map[string]int) (int, error) {
	if names != nil {
		if v, ok := names[strings.ToLower(s)]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", s)
	}
	if v < lo || v > hi {
		return 0, fmt.Errorf("value %d out of range %d-%d", v, lo, hi)
	}
	return v, nil
}

func (s *Spec) dayMatches(y int, m time.Month, d int, loc *time.Location) bool {
	domOK := s.dom&(1<<uint(d)) != 0
	wd := time.Date(y, m, d, 12, 0, 0, 0, loc).Weekday()
	dowOK := s.dow&(1<<uint(wd)) != 0
	if !s.domStar && !s.dowStar {
		return domOK || dowOK
	}
	return domOK && dowOK
}

const maxSearchDays = 366 * 9

func (s *Spec) NextAfter(t time.Time) time.Time {
	loc := t.Location()
	y, mo, d := t.Date()
	h, mi, _ := t.Clock()

	startH, startM := h, mi+1
	if startM == 60 {
		startH, startM = h+1, 0
	}

	for i := 0; i < maxSearchDays; i++ {
		day := time.Date(y, mo, d+i, 12, 0, 0, 0, loc)
		dy, dm, dd := day.Date()
		if s.month&(1<<uint(dm)) == 0 || !s.dayMatches(dy, dm, dd, loc) {
			continue
		}
		hFrom, mFrom := 0, 0
		if i == 0 {
			hFrom, mFrom = startH, startM
		}
		for hh := hFrom; hh < 24; hh++ {
			if s.hour&(1<<uint(hh)) == 0 {
				continue
			}
			mStart := 0
			if hh == hFrom {
				mStart = mFrom
			}
			for mm := mStart; mm < 60; mm++ {
				if s.minute&(1<<uint(mm)) == 0 {
					continue
				}
				cand := time.Date(dy, dm, dd, hh, mm, 0, 0, loc)
				if cand.Hour() != hh || cand.Minute() != mm {
					continue
				}
				if !cand.After(t) {
					continue
				}
				return cand
			}
		}
	}
	return time.Time{}
}
