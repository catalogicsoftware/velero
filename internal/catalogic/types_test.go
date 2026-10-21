package catalogic

import (
	"testing"
	"time"
)

// TestTimeFormatRendersDayOfMonth guards against the "2006-01-06" typo class,
// where the two-digit-year token "06" is mistakenly used in the day position.
// A layout-shaped bug like this survives code review because it still looks
// like a date; only a value-level assertion on a known instant catches it.
//
// The chosen instant deliberately has a day-of-month (05) that differs from
// year%100 (26), month (03), hour (17), minute (41), and second (09) — so a
// token transposition in any position produces an observably wrong string.
func TestTimeFormatRendersDayOfMonth(t *testing.T) {
	fixed := time.Date(2026, time.March, 5, 17, 41, 9, 0, time.UTC)

	got := fixed.Format(TimeFormat)
	want := "2026-03-05 17:41:09 UTC: "

	if got != want {
		t.Errorf("TimeFormat rendered %q, want %q (check for 06/02/01 token mix-ups)", got, want)
	}
}
