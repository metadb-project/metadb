package stop

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/metadb-project/metadb/cmd/internal/eout"
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
	var pid int
	if pid, err = process.ReadPIDFile(opt.Datadir); err != nil {
		eout.Verbose("file not found: %s", util.SystemPIDFileName(opt.Datadir))
		eout.Verbose("server may already have stopped")
		return nil
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
				fmt.Fprintf(os.Stderr, " failed\n")
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
					fmt.Fprintf(os.Stderr, ".")
				}
				time.Sleep(100 * time.Millisecond)
				continue
			default:
				break
			}
		}
		if n%10 == 0 && eout.EnableTrace {
			fmt.Fprintf(os.Stderr, ".")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if eout.EnableTrace {
		fmt.Fprintf(os.Stderr, " done\n")
	}
	eout.Verbose("server stopped")
	return nil
}
