package process

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/metadb-project/metadb/cmd/metadb/util"
)

var stop int32

func SetStop() {
	atomic.StoreInt32(&stop, 1)
}

func Stop() bool {
	return atomic.LoadInt32(&stop) == 1
}

func ReadPIDFile(datadir string) (int, error) {
	var err error
	var f *os.File
	if f, err = os.Open(util.SystemPIDFileName(datadir)); err != nil {
		return 0, err
	}
	var pid int
	var n int
	if n, err = fmt.Fscanf(f, "%d", &pid); err != nil {
		return 0, err
	}
	if n != 1 {
		return 0, fmt.Errorf("unable to read data from file: %s", util.SystemPIDFileName(datadir))
	}
	if pid <= 0 {
		return 0, fmt.Errorf("invalid data in file: %s", util.SystemPIDFileName(datadir))
	}
	return pid, nil
}

func WritePIDFile(datadir string) error {
	var err error
	var f *os.File
	if f, err = os.OpenFile(util.SystemPIDFileName(datadir), os.O_RDWR|os.O_CREATE, util.ModePermRW); err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)
	if _, err = f.WriteString(fmt.Sprintf("%d\n", os.Getpid())); err != nil {
		return err
	}
	return nil
}

func RemovePIDFile(datadir string) {
	_ = os.Remove(util.SystemPIDFileName(datadir))
}

/*func PIDFileExists(datadir string) (bool, error) {
	var err error
	var e bool
	if e, err = util.FileExists(util.SystemPIDFileName(datadir)); err != nil {
		return false, err
	}
	return e, nil
}
*/
