package stop

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/metadb-project/metadb/cmd/metadb/eout"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Stop(opt *option.Stop) error {
	var err error
	if opt.Datadir == "" {
		return fmt.Errorf("data directory not specified")
	}
	var ok bool
	if ok, err = util.FileExists(opt.Datadir); err != nil {
		return fmt.Errorf("reading data directory: %s", opt.Datadir)
	}
	if !ok {
		return fmt.Errorf("data directory not found: %s", opt.Datadir)
	}
	lockfile := util.SystemPIDFileName(opt.Datadir)
	fexists, err := util.FileExists(lockfile)
	if err != nil {
		return fmt.Errorf("reading lock file %q: %s", lockfile, err)
	}
	if !fexists {
		eout.Verbose("lock file %q not found", lockfile)
		eout.Verbose("server may already have stopped")
		return nil
	}
	pid, err := process.ReadPIDFile(opt.Datadir)
	if err != nil {
		return fmt.Errorf("reading lock file %q: %s", lockfile, err)
	}
	if err = syscall.Kill(pid, syscall.SIGTERM); err != nil {
		eout.Verbose("process not found: %d", pid)
		eout.Verbose("server does not appear to be running")
		return nil
	}
	eout.Verbose("waiting for server to shut down")
	var n int
	for {
		n++
		if n > 600 {
			if eout.EnableTrace {
				_, _ = fmt.Fprintf(os.Stderr, " failed\n")
			}
			return fmt.Errorf("server does not shut down")
		}
		var p *os.Process
		if p, err = os.FindProcess(pid); err != nil {
			break
		}
		if err = p.Signal(syscall.Signal(0)); err != nil {
			var errno syscall.Errno
			var ok bool
			errno, ok = err.(syscall.Errno)
			if !ok {
				break
			}
			switch errno {
			case syscall.EPERM:
				if n%10 == 0 && eout.EnableTrace {
					_, _ = fmt.Fprintf(os.Stderr, ".")
				}
				time.Sleep(100 * time.Millisecond)
				continue
			default:
				break
			}
		}
		if n%10 == 0 && eout.EnableTrace {
			_, _ = fmt.Fprintf(os.Stderr, ".")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if eout.EnableTrace {
		_, _ = fmt.Fprintf(os.Stderr, " done\n")
	}
	eout.Verbose("server stopped")
	return nil
}
