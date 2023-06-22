//go:build linux

// setTCPOption is unused on Darwin at the moment, so linter complains about
// dead code without a build pragma

package tws

import (
	"fmt"
	"net"
	"syscall"
)

func setTCPOption(conn net.Conn, option, value int) error {
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		return fmt.Errorf("failed to set TCP socket option %d: %w", option, err)
	}

	err = raw.Control(func(fd uintptr) {
		err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, option, value)
	})
	if err != nil {
		return fmt.Errorf("failed to set TCP socket option %d: %w", option, err)
	}

	return nil
}
