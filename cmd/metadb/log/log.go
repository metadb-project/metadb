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

func Fatal(format string, v ...interface{}) {
	if DisableColor {
		//std.log.Printf("FATAL: "+format, v...)
		printf(nil, "FATAL", format, v...)
	} else {
		//var c = color.Fatal.SprintFunc()
		//std.log.Printf(c("FATAL:")+" "+format, v...)
		printf(color.Fatal, "FATAL", format, v...)
	}
}

func Error(format string, v ...interface{}) {
	if DisableColor {
		//std.log.Printf("ERROR: "+format, v...)
		printf(nil, "ERROR", format, v...)
	} else {
		//var c = color.Error.SprintFunc()
		//std.log.Printf(c("ERROR: ")+format, v...)
		printf(color.Error, "ERROR", format, v...)
	}
}

func Warning(format string, v ...interface{}) {
	if DisableColor {
		//std.log.Printf("WARNING: "+format, v...)
		printf(nil, "WARNING", format, v...)
	} else {
		//var c = color.Warning.SprintFunc()
		//std.log.Printf(c("WARNING: ")+format, v...)
		printf(color.Warning, "WARNING", format, v...)
	}
}

func Info(format string, v ...interface{}) {
	//std.log.Printf("INFO: "+format, v...)
	printf(nil, "INFO", format, v...)
}

func Debug(format string, v ...interface{}) {
	if !std.logDebug && !std.logTrace {
		return
	}
	//std.log.Printf("DEBUG: "+format, v...)
	printf(nil, "DEBUG", format, v...)
}

func Trace(format string, v ...interface{}) {
	if !std.logTrace {
		return
	}
	//std.log.Printf("TRACE: "+format, v...)
	printf(nil, "TRACE", format, v...)
}

func P(format string, v ...interface{}) {
	if DisableColor {
		//std.log.Printf("PRINT: "+format, v...)
		printf(nil, "PRINT", format, v...)
	} else {
		//var c = color.P.SprintFunc()
		//std.log.Printf(c("PRINT:")+" "+format, v...)
		printf(color.P, "PRINT", format, v...)
	}
}

func printf(c *fcolor.Color, level string, format string, v ...interface{}) {
	var msg = fmt.Sprintf(format, v...)
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
