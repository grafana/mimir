package sdnotify

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// ErrSdNotifyNoSocket is the error returned when the NOTIFY_SOCKET does not exist.
var ErrSdNotifyNoSocket = errors.New("No socket")

// Ready sends READY=1 to the systemd notify socket.
func Ready() error {
	return SdNotify("READY=1")
}

// Stopping sends STOPPING=1 to the systemd notify socket.
func Stopping() error {
	return SdNotify("STOPPING=1")
}

// Errno sends ERRNO=? to the systemd notify socket.
func Errno(errno int) error {
	return SdNotify(fmt.Sprintf("ERRNO=%d", errno))
}

// Status sends STATUS=? to the systemd notify socket.
func Status(status string) error {
	return SdNotify("STATUS=" + status)
}

// Watchdog sends WATCHDOG=1 to the systemd notify socket.
func Watchdog() error {
	return SdNotify("WATCHDOG=1")
}

// ErrWatchdogUsecNotSet is returned when the WATCHDOG_USEC env variable is
// not set.
var ErrWatchdogUsecNotSet = errors.New("WATCHDOG_USEC env variable is not set")

// WatchdogInterval finds the watchdog interval set by systemd.
func WatchdogInterval() (time.Duration, error) {
	ivalUsecStr, ok := os.LookupEnv("WATCHDOG_USEC")
	if !ok {
		return 0, ErrWatchdogUsecNotSet
	}

	ivalUsec, err := strconv.ParseUint(ivalUsecStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return time.Duration(ivalUsec) * time.Microsecond, nil
}

// DoWatchdog runs check twice every WatchdogInterval until the context closes.
// If check returns nil then the systemd watchdog is called.
func DoWatchdog(
	ctx context.Context,
	isOk func() bool,
) error {
	ival, err := WatchdogInterval()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(ival / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if isOk() {
				Watchdog()
			}
		}
	}
}
