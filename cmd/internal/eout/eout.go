package eout

import (
	"fmt"
	"os"

	"github.com/nazgaret/metadb/cmd/internal/color"
)

var EnableVerbose bool
var EnableTrace bool

func Init(program string) {
	prog = program
}

func Error(format string, v ...interface{}) {
	locus()
	_, _ = color.Error.Fprint(std, "error: ")
	message(format, v...)
}

func Warning(format string, v ...interface{}) {
	locus()
	_, _ = color.Warning.Fprint(std, "warning: ")
	message(format, v...)
}

func Info(format string, v ...interface{}) {
	locus()
	message(format, v...)
}

func Verbose(format string, v ...interface{}) {
	if !EnableVerbose && !EnableTrace {
		return
	}
	locus()
	message(format, v...)
}

func Trace(format string, v ...interface{}) {
	if !EnableTrace {
		return
	}
	message(format, v...)
}

/*func P(format string, v ...interface{}) {
	locus()
	_, _ = color.P.Fprint(std, "print:")
	message(" "+format, v...)
}
*/

func locus() {
	_, _ = color.Locus.Fprint(std, fmt.Sprintf("%s: ", prog))
}

func message(format string, v ...interface{}) {
	_, _ = fmt.Fprintf(std, format+"\n", v...)
}

var std *os.File = os.Stderr
var prog string
