package log

import (
	"fmt"
	"io"
	glog "log"
	"os"
	"time"

	fcolor "github.com/fatih/color"
	"github.com/metadb-project/metadb/cmd/internal/color"
	"github.com/metadb-project/metadb/cmd/mdb/util"
)

var DisableColor bool

type Log struct {
	log      *glog.Logger
	logDebug bool
	logTrace bool
}

//const (
//        logtypeFatal   = 0
//        logtypeError   = 1
//        logtypeWarning = 2
//        logtypeInfo    = 3
//        logtypeDebug   = 4
//        logtypeTrace   = 5
//)

var std *Log
var csv *Log

func Init(out, csvout io.Writer, logDebug bool, logTrace bool) {
	if out != nil {
		std = &Log{
			log:      glog.New(out, "", 0),
			logDebug: logDebug,
			logTrace: logTrace,
		}
	}
	if csvout != nil {
		csv = &Log{
			log:      glog.New(csvout, "", 0),
			logDebug: logDebug,
			logTrace: logTrace,
		}
	}
}

func Fatal(format string, args ...interface{}) {
	if DisableColor {
		//std.log.Printf("FATAL: "+format, args...)
		printf(nil, "FATAL", format, args...)
	} else {
		//var c = color.Fatal.SprintFunc()
		//std.log.Printf(c("FATAL:")+" "+format, args...)
		printf(color.Fatal, "FATAL", format, args...)
	}
}

func Error(format string, args ...interface{}) {
	if DisableColor {
		//std.log.Printf("ERROR: "+format, args...)
		printf(nil, "ERROR", format, args...)
	} else {
		//var c = color.Error.SprintFunc()
		//std.log.Printf(c("ERROR: ")+format, args...)
		printf(color.Error, "ERROR", format, args...)
	}
}

func Warning(format string, args ...interface{}) {
	if DisableColor {
		//std.log.Printf("WARNING: "+format, args...)
		printf(nil, "WARNING", format, args...)
	} else {
		//var c = color.Warning.SprintFunc()
		//std.log.Printf(c("WARNING: ")+format, args...)
		printf(color.Warning, "WARNING", format, args...)
	}
}

func Info(format string, args ...interface{}) {
	//std.log.Printf("INFO: "+format, args...)
	printf(nil, "INFO", format, args...)
}

func Debug(format string, args ...interface{}) {
	if !std.logDebug && !std.logTrace {
		return
	}
	//std.log.Printf("DEBUG: "+format, args...)
	printf(nil, "DEBUG", format, args...)
}

func Trace(format string, args ...interface{}) {
	if !std.logTrace {
		return
	}
	//std.log.Printf("TRACE: "+format, args...)
	printf(nil, "TRACE", format, args...)
}

/*func P(format string, args ...interface{}) {
	if DisableColor {
		//std.log.Printf("PRINT: "+format, args...)
		printf(nil, "PRINT", format, args...)
	} else {
		//var c = color.P.SprintFunc()
		//std.log.Printf(c("PRINT:")+" "+format, args...)
		printf(color.P, "PRINT", format, args...)
	}
}
*/

func printf(c *fcolor.Color, level string, format string, args ...interface{}) {
	var msg = fmt.Sprintf(format, args...)
	var n = time.Now().UTC()
	var now = n.Format("2006-01-02 15:04:05 MST")
	var nowRFC = n.Format(time.RFC3339)
	// Main log
	if std != nil {
		if DisableColor || c == nil {
			std.log.Printf("%s  %s  %s", now, level+":", msg)
		} else {
			var cf = c.SprintFunc()
			std.log.Printf("%s  %s  %s", now, cf(level+":"), msg)
		}
	}
	// CSV log
	if csv != nil {
		csv.log.Printf("%q,%q,%q", nowRFC, level, msg)
	}
}

// Source log

type SourceLog struct {
	srclog *glog.Logger
}

func NewSourceLog(logfile string) (*SourceLog, error) {
	var err error
	var f *os.File
	if f, err = OpenLogFile(logfile); err != nil {
		return nil, err
	}
	return &SourceLog{glog.New(f, "", 0)}, nil
}

func (s *SourceLog) Log(msg string) {
	s.srclog.Printf("%s", msg)
}

func OpenLogFile(logfile string) (*os.File, error) {
	var f *os.File
	var err error
	if f, err = os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, util.ModePermRW); err != nil {
		return nil, err
	}
	return f, nil
}
