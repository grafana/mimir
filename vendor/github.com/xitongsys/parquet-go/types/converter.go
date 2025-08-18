package types

import (
	"encoding/binary"
	"math"
	"math/big"
	"strconv"
	"time"
)

func TimeToTIME_MILLIS(t time.Time, adjustedToUTC bool) int64 {
	return TimeToTIME_MICROS(t, adjustedToUTC) / time.Millisecond.Microseconds()
}

func TimeToTIME_MICROS(t time.Time, adjustedToUTC bool) int64 {
	if adjustedToUTC {
		tu := t.UTC()
		h, m, s, ns := int64(tu.Hour()), int64(tu.Minute()), int64(tu.Second()), int64(tu.Nanosecond())
		nanos := h*time.Hour.Nanoseconds() + m*time.Minute.Nanoseconds() + s*time.Second.Nanoseconds() + ns*time.Nanosecond.Nanoseconds()
		return nanos / time.Microsecond.Nanoseconds()

	} else {
		h, m, s, ns := int64(t.Hour()), int64(t.Minute()), int64(t.Second()), int64(t.Nanosecond())
		nanos := h*time.Hour.Nanoseconds() + m*time.Minute.Nanoseconds() + s*time.Second.Nanoseconds() + ns*time.Nanosecond.Nanoseconds()
		return nanos / time.Microsecond.Nanoseconds()
	}
}

func TimeToTIMESTAMP_MILLIS(t time.Time, adjustedToUTC bool) int64 {
	return TimeToTIMESTAMP_MICROS(t, adjustedToUTC) / time.Millisecond.Microseconds()
}

func TIMESTAMP_MILLISToTime(millis int64, adjustedToUTC bool) time.Time {
	return TIMESTAMP_MICROSToTime(millis*time.Millisecond.Microseconds(), adjustedToUTC)
}

func TimeToTIMESTAMP_MICROS(t time.Time, adjustedToUTC bool) int64 {
	return TimeToTIMESTAMP_NANOS(t, adjustedToUTC) / time.Microsecond.Nanoseconds()
}

func TIMESTAMP_MICROSToTime(micros int64, adjustedToUTC bool) time.Time {
	return TIMESTAMP_NANOSToTime(micros*time.Microsecond.Nanoseconds(), adjustedToUTC)
}

func TimeToTIMESTAMP_NANOS(t time.Time, adjustedToUTC bool) int64 {
	if adjustedToUTC {
		return t.UnixNano()
	} else {
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, t.Location())
		return t.Sub(epoch).Nanoseconds()
	}
}

func TIMESTAMP_NANOSToTime(nanos int64, adjustedToUTC bool) time.Time {
	if adjustedToUTC {
		return time.Unix(0, nanos).UTC()

	} else {
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
		t := epoch.Add(time.Nanosecond * time.Duration(nanos))
		return t
	}
}

//From Spark
//https://github.com/apache/spark/blob/b9f2f78de59758d1932c1573338539e485a01112/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala#L47
const (
	JULIAN_DAY_OF_EPOCH int64 = 2440588
	MICROS_PER_DAY      int64 = 3600 * 24 * 1000 * 1000
)

//From Spark
//https://github.com/apache/spark/blob/b9f2f78de59758d1932c1573338539e485a01112/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala#L180
func toJulianDay(t time.Time) (int32, int64) {
	utc := t.UTC()
	nanos := utc.UnixNano()
	micros := nanos / time.Microsecond.Nanoseconds()

	julianUs := micros + JULIAN_DAY_OF_EPOCH*MICROS_PER_DAY
	days := int32(julianUs / MICROS_PER_DAY)
	us := (julianUs % MICROS_PER_DAY) * 1000
	return days, us
}

//From Spark
//https://github.com/apache/spark/blob/b9f2f78de59758d1932c1573338539e485a01112/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala#L170
func fromJulianDay(days int32, nanos int64) time.Time {
	nanos = ((int64(days)-JULIAN_DAY_OF_EPOCH)*MICROS_PER_DAY + nanos/1000) * 1000
	sec, nsec := nanos/time.Second.Nanoseconds(), nanos%time.Second.Nanoseconds()
	t := time.Unix(sec, nsec)
	return t.UTC()
}

func TimeToINT96(t time.Time) string {
	days, nanos := toJulianDay(t)

	bs1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs1, uint64(nanos))

	bs2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs2, uint32(days))

	bs := append(bs1, bs2...)
	return string(bs)
}

func INT96ToTime(int96 string) time.Time {
	nanos := binary.LittleEndian.Uint64([]byte(int96[:8]))
	days := binary.LittleEndian.Uint32([]byte(int96[8:]))

	return fromJulianDay(int32(days), int64(nanos))
}

func DECIMAL_INT_ToString(dec int64, precision int, scale int) string {
	s := int(math.Pow10(scale))
	integer, fraction := int(dec)/s, int(dec)%s
	ans := strconv.Itoa(integer)
	if scale > 0 {
		ans += "." + strconv.Itoa(fraction)
	}
	return ans
}

func DECIMAL_BYTE_ARRAY_ToString(dec []byte, precision int, scale int) string {
	a := new(big.Int)
	a.SetBytes(dec)
	sa := a.Text(10)

	if scale > 0 {
		ln := len(sa)
		sa = sa[:ln-scale] + "." + sa[ln-scale:]
	}
	return sa
}
