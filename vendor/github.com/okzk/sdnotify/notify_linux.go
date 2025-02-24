package sdnotify

import (
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// SdNotify sends a specified string to the systemd notification socket.
func SdNotify(state string) error {
	name := os.Getenv("NOTIFY_SOCKET")
	if name == "" {
		return ErrSdNotifyNoSocket
	}

	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: name, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Write([]byte(state))
	return err
}

// Reloading sends RELOADING=1\nMONOTONIC_USEC=<monotonic_time> to the
// systemd notify socket.
func Reloading() error {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		return fmt.Errorf("getting monotonic time: %w", err)
	}
	nsecs := ts.Sec*1e09 + ts.Nsec
	return SdNotify(fmt.Sprintf("RELOADING=1\nMONOTONIC_USEC=%d",
		nsecs/1000))
}
