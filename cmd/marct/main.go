package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/metadb-project/metadb/cmd/internal/marct"
)

var fullUpdateFlag = flag.Bool("f", false, "Perform full update even if incremental update is available")
var incUpdateFlag = flag.Bool("i", false, "[option no longer supported]")
var datadirFlag = flag.String("D", "", "Data directory")
var ldpUserFlag = flag.String("u", "", "User to be granted select privileges")
var noTrigramIndexFlag = flag.Bool("T", false, "[option no longer supported]")
var trigramIndexFlag = flag.Bool("t", false, "Create trigram index on content column")
var noIndexesFlag = flag.Bool("I", false, "Disable creation of all indexes")
var verboseFlag = flag.Bool("v", false, "Enable verbose output")
var csvFilenameFlag = flag.String("c", "", "Write output to CSV file instead of a database")
var srsRecordsFlag = flag.String("r", "", "Name of table containing MARC records to read")
var srsMarcFlag = flag.String("m", "", "Name of table containing MARC (JSON) data to read")
var srsMarcAttrFlag = flag.String("j", "", "Name of column containing MARC JSON data")
var metadbFlag = flag.Bool("M", false, "Metadb compatibility")
var helpFlag = flag.Bool("h", false, "Help for marct")

var program = "marct"

func main() {
	flag.Parse()
	if len(flag.Args()) > 0 {
		printerr("invalid argument: %s", flag.Arg(0))
		os.Exit(2)
	}
	if *helpFlag || *datadirFlag == "" {
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %s:\n", program)
		flag.PrintDefaults()
		if *helpFlag {
			return
		} else {
			os.Exit(2)
		}
	}
	if *incUpdateFlag {
		printerr("-i option no longer supported")
		os.Exit(1)
	}
	if *noTrigramIndexFlag {
		printerr("-T option no longer supported")
		os.Exit(1)
	}
	users := make([]string, 0)
	if *ldpUserFlag != "" {
		users = append(users, *ldpUserFlag)
	}
	verbose := 1
	if *verboseFlag {
		verbose = 2
	}
	opt := &marct.TransformOptions{
		FullUpdate:   *fullUpdateFlag,
		Datadir:      *datadirFlag,
		Users:        users,
		TrigramIndex: *trigramIndexFlag,
		NoIndexes:    *noIndexesFlag,
		Verbose:      verbose,
		CSVFileName:  *csvFilenameFlag,
		SRSRecords:   *srsRecordsFlag,
		SRSMarc:      *srsMarcFlag,
		SRSMarcAttr:  *srsMarcAttrFlag,
		Metadb:       *metadbFlag,
		PrintErr:     printerr,
	}
	if err := marct.Run(opt); err != nil {
		printerr("%s", err)
		os.Exit(1)
	}
}

func printerr(format string, v ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", program, fmt.Sprintf(format, v...))
}
