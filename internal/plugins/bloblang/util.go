package bloblang

import (
	"math"
	"sort"
	"time"

	"github.com/warpstreamlabs/bento/public/bloblang"
)

func GetNumDaysInMonth(t time.Time) int {
	t = time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, time.UTC)
	return 32 - t.Day()
}

func RegisterGetNumDaysInMonth() {
	pSpec := bloblang.NewPluginSpec().
		Description("Find the number of days in the month of the date given.")

	if err := bloblang.RegisterMethodV2("num_days_in_month", pSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.TimestampMethod(func(t time.Time) (any, error) {
			return GetNumDaysInMonth(t), nil
		}), nil
	}); err != nil {
		panic(err)
	}
}

func RegisterCenterSliceSameMonth() {
	pSpec := bloblang.NewPluginSpec().
		Description("Create slice of dates centered around this date. All in the same month and not before min.").
		Param(bloblang.NewInt64Param("take")).
		Param(bloblang.NewTimestampParam("min"))

	if err := bloblang.RegisterMethodV2("center_slice_same_month", pSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.TimestampMethod(func(t time.Time) (any, error) {
			take, err := args.GetInt64("take")
			if err != nil {
				return nil, err
			}
			minDate, err := args.GetTimestamp("min")
			if err != nil {
				return nil, err
			}
			return CenterSliceSameMonth(t, take, minDate), nil
		}), nil
	}); err != nil {
		panic(err)
	}
}

func CenterSliceSameMonth(t time.Time, take int64, minDate time.Time) []time.Time {
	var result []time.Time
	result = append(result, t)
	next := t
	previous := t
	for i := 0; ; i++ {
		if !next.IsZero() && i%2 == 1 {
			next = nextDate(t.Month(), next)
			if !next.IsZero() {
				result = append(result, next)
			}
		} else if !previous.IsZero() {
			previous = previousDate(t.Month(), previous, minDate)
			if !previous.IsZero() {
				result = append(result, previous)
			}
		} else if next.IsZero() && previous.IsZero() {
			break
		}
	}
	return result
}

func nextDate(month time.Month, current time.Time) time.Time {
	next := current.AddDate(0, 0, 1)
	if next.Weekday() == time.Saturday {
		next = next.AddDate(0, 0, 2)
	}
	if next.Month() != month {
		return time.Time{}
	}
	return next
}

func previousDate(month time.Month, current time.Time, minDate time.Time) time.Time {
	previous := current.AddDate(0, 0, -1)
	if previous.Weekday() == time.Sunday {
		previous = previous.AddDate(0, 0, -2)
	}
	if previous.Month() != month || previous.Before(minDate) {
		return time.Time{}
	}
	return previous
}

func timediff_days_absolute(a string, b string, f string) (float64, error) {
	t1, err := time.ParseInLocation(f, a, time.UTC)
	if err != nil {
		return 0, err
	}
	t2, err := time.ParseInLocation(f, b, time.UTC)
	if err != nil {
		return 0, err
	}
	if t1.After(t2) {
		t1, t2 = t2, t1
	}
	return math.Ceil(t2.Sub(t1).Hours() / 24.0), nil
}

func RegisterTimeDiffDaysAbsolute() {
	pSpec := bloblang.NewPluginSpec().
		Description("Returns number of days between this date and argument given").
		Param(bloblang.NewStringParam("b")).
		Param(bloblang.NewStringParam("f"))

	if err := bloblang.RegisterMethodV2("timediff_days_absolute", pSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.StringMethod(func(s string) (any, error) {
			b, err := args.GetString("b")
			if err != nil {
				return nil, err
			}
			f, err := args.GetString("f")
			if err != nil {
				return nil, err
			}
			return timediff_days_absolute(s, b, f)
		}), nil
	}); err != nil {
		panic(err)
	}
}

func find_nearest_date_from_slice(s []time.Time, date time.Time) time.Time {
	sort.Slice(s, func(i, j int) bool {
		return s[i].Before(s[j])
	})
	for _, d := range s {
		if d.Equal(date) || d.After(date) {
			return d
		}
	}
	return time.Time{}
}

func RegisterFindNearestDateFromSlice() {
	pSpec := bloblang.NewPluginSpec().
		Description("Returns the date from slice nearest the date in question, or time zero if none found.").
		Param(bloblang.NewTimestampParam("date"))

	if err := bloblang.RegisterMethodV2("find_nearest_date_from_slice", pSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.ArrayMethod(func(a []any) (any, error) {
			date, err := args.GetTimestamp("date")
			if err != nil {
				return nil, err
			}
			dates := make([]time.Time, len(a))
			for i, v := range a {
				dates[i] = v.(time.Time)
			}
			return find_nearest_date_from_slice(dates, date), nil
		}), nil
	}); err != nil {
		panic(err)
	}
}
