package process

import (
	"fmt"
	"os"
	"syscall"

	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func IsServerRunning(datadir string) (bool, int, error) {
	// check for lock file
	lockfile := util.SystemPIDFileName(datadir)
	fexists, err := util.FileExists(lockfile)
	if err != nil {
		return false, 0, fmt.Errorf("reading lock file %q: %s", lockfile, err)
	}
	if !fexists {
		return false, 0, nil
	}
	// read pid
	pid, err := ReadPIDFile(datadir)
	if err != nil {
		return false, 0, fmt.Errorf("reading lock file %q: %s", lockfile, err)
	}
	// check for running process
	p, err := os.FindProcess(pid)
	if err != nil {
		return false, 0, nil
	}
	err = p.Signal(syscall.Signal(0))
	if err != nil {
		errno, ok := err.(syscall.Errno)
		if !ok {
			return false, 0, nil
		}
		if errno == syscall.EPERM {
			return true, pid, nil
		} else {
			return false, 0, nil
		}
	}
	return true, pid, nil
}
