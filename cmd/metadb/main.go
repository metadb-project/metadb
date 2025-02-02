package main

import (
	"fmt"
	"os"

	"github.com/metadb-project/metadb/cmd/metadb/color"
	"github.com/metadb-project/metadb/cmd/metadb/config"
	"github.com/metadb-project/metadb/cmd/metadb/dsync"
	"github.com/metadb-project/metadb/cmd/metadb/eout"
	"github.com/metadb-project/metadb/cmd/metadb/initsys"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/server"
	"github.com/metadb-project/metadb/cmd/metadb/stop"
	"github.com/metadb-project/metadb/cmd/metadb/upgrade"
	"github.com/metadb-project/metadb/cmd/metadb/util"
	"github.com/spf13/cobra"
)

var program = "metadb"

var colorMode string
var devMode bool

var colorInitialized bool

var defaultPort = "8550"

func main() {
	colorMode = os.Getenv("METADB_COLOR")
	devMode = os.Getenv("METADB_DEV") == "on"
	metadbMain()
}

func metadbMain() {
	// Initialize error output
	eout.Init(program)
	if config.Experimental {
		eout.Info("experimental mode")
	}
	// Run
	var err error
	if err = run(); err != nil {
		if !colorInitialized {
			color.NeverColor()
		}
		eout.Error("%s", err)
		os.Exit(1)
	}
}

func run() error {
	var globalOpt = option.Global{}
	var initOpt = option.Init{}
	var upgradeOpt = option.Upgrade{}
	var serverOpt = option.Server{}
	var stopOpt = option.Stop{}
	var syncOpt = option.Sync{}
	var endSyncOpt = option.EndSync{}
	var migrateOpt = option.Migrate{}
	var logfile, csvlogfile string

	var cmdInit = &cobra.Command{
		Use: "init",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			initOpt.Global = globalOpt
			if err = initsys.InitSys(&initOpt); err != nil {
				return err
			}
			return nil
		},
	}
	cmdInit.SetHelpFunc(help)
	_ = dirFlag(cmdInit, &initOpt.Datadir)
	//_ = databaseFlag(cmdInit, &initOpt.DatabaseURI)
	_ = verboseFlag(cmdInit, &eout.EnableVerbose)
	_ = traceFlag(cmdInit, &eout.EnableTrace)

	var cmdUpgrade = &cobra.Command{
		Use: "upgrade",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			upgradeOpt.Global = globalOpt
			// if err = sysdb.Init(util.SysdbFileName(upgradeOpt.Datadir)); err != nil {
			// 	return err
			// }
			if err = upgrade.Upgrade(&upgradeOpt); err != nil {
				return err
			}
			// err = sysdb.Close()
			// if err != nil {
			// 	log.Error("%s", err)
			// }
			return nil
		},
	}
	cmdUpgrade.SetHelpFunc(help)
	_ = dirFlag(cmdUpgrade, &upgradeOpt.Datadir)
	_ = forceFlag(cmdUpgrade, &upgradeOpt.Force)
	_ = verboseFlag(cmdUpgrade, &eout.EnableVerbose)
	_ = traceFlag(cmdUpgrade, &eout.EnableTrace)

	var cmdStart = &cobra.Command{
		Use: "start",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			serverOpt.Global = globalOpt
			if err = validateServerOptions(&serverOpt); err != nil {
				return err
			}
			// if err = sysdb.Init(util.SysdbFileName(serverOpt.Datadir)); err != nil {
			// 	return err
			// }
			var logf, csvlogf *os.File
			if logf, csvlogf, err = setupLog(logfile, csvlogfile, serverOpt.Debug, serverOpt.Trace); err != nil {
				return err
			}
			//if serverOpt.Port == "" {
			//        serverOpt.Port = metadbAdminPort
			//}
			serverOpt.Listen = "127.0.0.1"
			if err = server.Start(&serverOpt); err != nil {
				return fatal(err, logf, csvlogf)
			}
			return nil
		},
	}
	cmdStart.SetHelpFunc(help)
	_ = dirFlag(cmdStart, &serverOpt.Datadir)
	_ = logFlag(cmdStart, &logfile)
	//_ = csvlogFlag(cmdStart, &csvlogfile)
	//_ = listenFlag(cmdStart, &serverOpt.Listen)
	_ = portFlag(cmdStart, &serverOpt.Port)
	//_ = certFlag(cmdStart, &serverOpt.TLSCert)
	//_ = keyFlag(cmdStart, &serverOpt.TLSKey)
	_ = uuoptFlag(cmdStart, &serverOpt.UUOpt)
	_ = debugFlag(cmdStart, &serverOpt.Debug)
	_ = traceLogFlag(cmdStart, &serverOpt.Trace)
	_ = noKafkaCommitFlag(cmdStart, &serverOpt.NoKafkaCommit)
	_ = logSourceFlag(cmdStart, &serverOpt.LogSource)
	//_ = noTLSFlag(cmdStart, &serverOpt.NoTLS)
	_ = memoryLimitFlag(cmdStart, &serverOpt.MemoryLimit)

	var cmdStop = &cobra.Command{
		Use: "stop",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			stopOpt.Global = globalOpt
			if err = stop.Stop(&stopOpt); err != nil {
				return err
			}
			return nil
		},
	}
	cmdStop.SetHelpFunc(help)
	_ = dirFlag(cmdStop, &stopOpt.Datadir)
	_ = verboseFlag(cmdStop, &eout.EnableVerbose)
	_ = traceFlag(cmdStop, &eout.EnableTrace)

	var cmdSync = &cobra.Command{
		Use: "sync",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			syncOpt.Global = globalOpt
			if err = dsync.Sync(&syncOpt); err != nil {
				return err
			}
			return nil
		},
	}
	cmdSync.SetHelpFunc(help)
	cmdSync.Flags().StringVar(&syncOpt.Source, "source", "", "")
	_ = cmdSync.MarkFlagRequired("source")
	_ = dirFlag(cmdSync, &syncOpt.Datadir)
	_ = forceFlag(cmdSync, &syncOpt.Force)
	// _ = forceAllFlag(cmdSync, &syncOpt.ForceAll)
	_ = verboseFlag(cmdSync, &eout.EnableVerbose)
	_ = traceFlag(cmdSync, &eout.EnableTrace)

	var cmdEndSync = &cobra.Command{
		Use: "endsync",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			endSyncOpt.Global = globalOpt
			if err = dsync.EndSync(&endSyncOpt); err != nil {
				return err
			}
			return nil
		},
	}
	cmdEndSync.SetHelpFunc(help)
	cmdEndSync.Flags().StringVar(&endSyncOpt.Source, "source", "", "")
	_ = cmdEndSync.MarkFlagRequired("source")
	_ = dirFlag(cmdEndSync, &endSyncOpt.Datadir)
	_ = forceFlag(cmdEndSync, &endSyncOpt.Force)
	// _ = forceAllFlag(cmdEndSync, &endSyncOpt.ForceAll)
	_ = verboseFlag(cmdEndSync, &eout.EnableVerbose)
	_ = traceFlag(cmdEndSync, &eout.EnableTrace)

	var cmdMigrate = &cobra.Command{
		Use: "migrate",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			migrateOpt.Global = globalOpt
			if err = upgrade.Migrate(&migrateOpt); err != nil {
				return err
			}
			return nil
		},
	}
	cmdMigrate.SetHelpFunc(help)
	cmdMigrate.Flags().StringVar(&migrateOpt.LDPConf, "ldpconf", "", "")
	_ = cmdMigrate.MarkFlagRequired("ldpconf")
	cmdMigrate.Flags().StringVar(&migrateOpt.Source, "source", "", "")
	_ = cmdMigrate.MarkFlagRequired("source")
	_ = dirFlag(cmdMigrate, &migrateOpt.Datadir)
	_ = traceFlag(cmdMigrate, &eout.EnableTrace)

	var cmdVersion = &cobra.Command{
		Use: "version",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("metadb version %s\n", util.MetadbVersion)
			return nil
		},
	}
	cmdVersion.SetHelpFunc(help)

	var rootCmd = &cobra.Command{
		Use:                "metadb",
		SilenceErrors:      true,
		SilenceUsage:       true,
		DisableSuggestions: true,
		CompletionOptions:  cobra.CompletionOptions{DisableDefaultCmd: true},
	}
	rootCmd.SetHelpFunc(help)
	// Redefine help flag without -h; so we can use it for something else.
	var helpFlag bool
	rootCmd.PersistentFlags().BoolVarP(&helpFlag, "help", "", false, "Help for metadb")
	//rootCmd.PersistentFlags().StringVar(&_, "admin", metadbAdminPort, ""+
	//        "admin port")
	//rootCmd.PersistentFlags().StringVar(&_, "client", metadbClientPort, ""+
	//        "client port")
	// Add commands.
	rootCmd.AddCommand(cmdStart, cmdStop, cmdInit, cmdUpgrade, cmdSync, cmdEndSync, cmdMigrate, cmdVersion)
	var err error
	if err = rootCmd.Execute(); err != nil {
		return err
	}

	return nil
}

var helpStart = "Start server\n"
var helpStop = "Shutdown server\n"
var helpInit = "Initialize new Metadb instance\n"
var helpUpgrade = "Upgrade Metadb instance to current version\n"
var helpSync = "Begin synchronization with a data source\n"
var helpEndSync = "End synchronization and remove leftover data\n"
var helpMigrate = "Migrate historical data from LDP\n"
var helpVersion = "Print metadb version\n"

func help(cmd *cobra.Command, commandLine []string) {
	_ = commandLine
	switch cmd.Use {
	case "metadb":
		fmt.Print("" +
			"Metadb server\n" +
			"\n" +
			"Usage:  metadb <command> <arguments>\n" +
			"\n" +
			"Commands:\n" +
			"  start                       - " + helpStart +
			"  stop                        - " + helpStop +
			"  init                        - " + helpInit +
			"  upgrade                     - " + helpUpgrade +
			"  sync                        - " + helpSync +
			"  endsync                     - " + helpEndSync +
			"  migrate                     - " + helpMigrate +
			"  version                     - " + helpVersion +
			"\n" +
			"Use \"metadb help <command>\" for more information about a command.\n")
	case "start":
		fmt.Print("" +
			helpStart +
			"\n" +
			"Usage:  metadb start <options>\n" +
			"\n" +
			"Options:\n" +
			dirFlag(nil, nil) +
			logFlag(nil, nil) +
			//csvlogFlag(nil, nil) +
			//listenFlag(nil, nil) +
			portFlag(nil, nil) +
			//certFlag(nil, nil) +
			//keyFlag(nil, nil) +
			uuoptFlag(nil, nil) +
			debugFlag(nil, nil) +
			//noTLSFlag(nil, nil) +
			traceLogFlag(nil, nil) +
			noKafkaCommitFlag(nil, nil) +
			logSourceFlag(nil, nil) +
			memoryLimitFlag(nil, nil) +
			"")
	case "stop":
		fmt.Print("" +
			helpStop +
			"\n" +
			"Usage:  metadb stop <options>\n" +
			"\n" +
			"Options:\n" +
			dirFlag(nil, nil) +
			verboseFlag(nil, nil) +
			traceFlag(nil, nil) +
			"")
	case "init":
		fmt.Print("" +
			helpInit +
			"\n" +
			"Usage:  metadb init <options>\n" +
			"\n" +
			"Options:\n" +
			dirFlag(nil, nil) +
			//databaseFlag(nil, nil) +
			verboseFlag(nil, nil) +
			traceFlag(nil, nil) +
			"")
	case "upgrade":
		fmt.Print("" +
			helpUpgrade +
			"\n" +
			"Usage:  metadb upgrade <options>\n" +
			"\n" +
			"Options:\n" +
			dirFlag(nil, nil) +
			forceFlag(nil, nil) +
			verboseFlag(nil, nil) +
			traceFlag(nil, nil) +
			"")
	case "sync":
		fmt.Print("" +
			helpSync +
			"\n" +
			"Usage:  metadb sync <options>\n" +
			"\n" +
			"Options:\n" +
			"      --source <s>            - Data source to synchronize\n" +
			dirFlag(nil, nil) +
			forceFlag(nil, nil) +
			// forceAllFlag(nil, nil) +
			verboseFlag(nil, nil) +
			traceFlag(nil, nil) +
			"")
	case "endsync":
		fmt.Print("" +
			helpEndSync +
			"\n" +
			"Usage:  metadb endsync <options>\n" +
			"\n" +
			"Options:\n" +
			"      --source <s>            - Data source to finish synchronizing\n" +
			dirFlag(nil, nil) +
			forceFlag(nil, nil) +
			// forceAllFlag(nil, nil) +
			verboseFlag(nil, nil) +
			traceFlag(nil, nil) +
			"")
	case "migrate":
		fmt.Print("" +
			helpMigrate +
			"\n" +
			"Usage:  metadb migrate <options>\n" +
			"\n" +
			"Options:\n" +
			"      --ldpconf <f>           - ldpconf.json file\n" +
			"      --source <s>            - Data source for creating new tables\n" +
			"  -D, --dir <d>               - Metadb data directory\n" +
			traceFlag(nil, nil) +
			"")
	case "version":
		fmt.Print("" +
			helpVersion +
			"\n" +
			"Usage:  metadb version\n")
	default:
	}
}

func verboseFlag(cmd *cobra.Command, verbose *bool) string {
	if cmd != nil {
		cmd.Flags().BoolVarP(verbose, "verbose", "v", false, "")
	}
	return "" +
		"  -v, --verbose               - Enable verbose output\n"
}

func uuoptFlag(cmd *cobra.Command, uuopt *bool) string {
	if cmd != nil {
		cmd.Flags().BoolVar(uuopt, "uuopt", false, "")
	}
	return "" +
		"      --uuopt                 - Enable \"unnecessary update\" optimization\n"
}

func debugFlag(cmd *cobra.Command, debug *bool) string {
	if cmd != nil {
		cmd.Flags().BoolVar(debug, "debug", false, "")
	}
	return "" +
		"      --debug                 - Enable detailed logging\n"
}

func traceFlag(cmd *cobra.Command, trace *bool) string {
	if devMode {
		if cmd != nil {
			cmd.Flags().BoolVar(trace, "trace", false, "")
		}
		return "" +
			"      --trace                 - Enable extremely verbose output\n"
	}
	return ""
}

func forceFlag(cmd *cobra.Command, force *bool) string {
	if cmd != nil {
		cmd.Flags().BoolVar(force, "force", false, "")
	}
	return "" +
		"      --force                 - Do not prompt for confirmation\n"
}

func forceAllFlag(cmd *cobra.Command, forceAll *bool) string {
	if cmd != nil {
		cmd.Flags().BoolVar(forceAll, "forceall", false, "")
	}
	return "" +
		"      --forceall              - Never prompt, even for warnings and safety\n" +
		"                                checks\n"
}

func noKafkaCommitFlag(cmd *cobra.Command, noKafkaCommit *bool) string {
	if devMode {
		if cmd != nil {
			cmd.Flags().BoolVar(noKafkaCommit, "nokcommit", false, "")
		}
		return "" +
			"      --nokcommit             - Do not commit Kafka offsets\n"
	}
	return ""
}

func logSourceFlag(cmd *cobra.Command, logfile *string) string {
	if devMode {
		if cmd != nil {
			cmd.Flags().StringVar(logfile, "logsource", "", "")
		}
		return "" +
			"      --logsource <f>         - Log source messages to file\n"
	}
	return ""
}

func traceLogFlag(cmd *cobra.Command, trace *bool) string {
	if devMode {
		if cmd != nil {
			cmd.Flags().BoolVar(trace, "trace", false, "")
		}
		return "" +
			"      --trace                 - Enable extremely detailed logging\n"
	}
	return ""
}

//func listenFlag(cmd *cobra.Command, listen *string) string {
//	if cmd != nil {
//		cmd.Flags().StringVar(listen, "listen", "", "")
//	}
//	return "" +
//		"      --listen <a>            - Address to listen on (default: 127.0.0.1)\n"
//}

func portFlag(cmd *cobra.Command, adminPort *string) string {
	if cmd != nil {
		cmd.Flags().StringVarP(adminPort, "port", "p", defaultPort, "")
	}
	return "" +
		"  -p, --port <p>              - Port to listen on (default: " + defaultPort + ")\n"
}

//func certFlag(cmd *cobra.Command, cert *string) string {
//	if cmd != nil {
//		cmd.Flags().StringVar(cert, "cert", "", "")
//	}
//	return "" +
//		"      --cert <f>              - File name of server certificate, including the\n" +
//		"                                CA's certificate and intermediates\n"
//}

//func keyFlag(cmd *cobra.Command, key *string) string {
//	if cmd != nil {
//		cmd.Flags().StringVar(key, "key", "", "")
//	}
//	return "" +
//		"      --key <f>               - File name of server private key\n"
//}

func logFlag(cmd *cobra.Command, logfile *string) string {
	if cmd != nil {
		cmd.Flags().StringVarP(logfile, "log", "l", "", "")
	}
	return "" +
		"  -l, --log <f>               - File name for server log output\n"
}

//func csvlogFlag(cmd *cobra.Command, logfile *string) string {
//	if cmd != nil {
//		cmd.Flags().StringVar(logfile, "csvlog", "", "")
//	}
//	return "" +
//		"      --csvlog <f>            - File name for server log CSV output\n"
//}

func dirFlag(cmd *cobra.Command, datadir *string) string {
	if cmd != nil {
		cmd.Flags().StringVarP(datadir, "dir", "D", "", "")
	}
	return "" +
		"  -D, --dir <d>               - Data directory\n"
}

/*func databaseFlag(cmd *cobra.Command, database *string) string {
	if cmd != nil {
		cmd.Flags().StringVar(database, "database", "", "")
	}
	return "" +
		"      --database <u>          - Database connection URI\n"
}
*/

//func noTLSFlag(cmd *cobra.Command, noTLS *bool) string {
//	if cmd != nil {
//		cmd.Flags().BoolVar(noTLS, "notls", false, "")
//	}
//	return "" +
//		"      --notls                 - Disable TLS in client connections [insecure,\n" +
//		"                                use for testing only]\n"
//}

func memoryLimitFlag(cmd *cobra.Command, memoryLimit *float64) string {
	if cmd != nil {
		cmd.Flags().Float64Var(memoryLimit, "memlimit", 1.0, "")
	}
	return "" +
		"      --memlimit <m>          - Approximate limit on memory usage in GiB\n" +
		"                                (default: 1.0)\n"
}

func setupLog(logfile, csvlogfile string, debug bool, trace bool) (*os.File, *os.File, error) {
	var err error
	var logf, csvlogf *os.File
	if logfile != "" || csvlogfile != "" {
		log.DisableColor = true
		if logfile != "" {
			if logf, err = log.OpenLogFile(logfile); err != nil {
				return nil, nil, err
			}
		}
		//if csvlogfile != "" {
		//	if csvlogf, err = log.OpenLogFile(csvlogfile); err != nil {
		//		return nil, nil, err
		//	}
		//}
		log.Init(logf, debug, trace)
		return logf, csvlogf, nil
	}
	log.Init(os.Stderr, debug, trace)
	return nil, nil, nil
}

func validateServerOptions(opt *option.Server) error {
	var err error
	// Require datadir specified
	if opt.Datadir == "" {
		return fmt.Errorf("data directory not specified")
	}
	// Require datadir exists
	var exists bool
	if exists, err = util.FileExists(opt.Datadir); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("data directory not found: %s", opt.Datadir)
	}
	// Require certificate and key specified if not loopback and
	// TLS not disabled
	if opt.Listen != "" && !opt.NoTLS {
		if opt.TLSCert == "" {
			return fmt.Errorf("server certificate not specified")
		}
		if opt.TLSKey == "" {
			return fmt.Errorf("server private key not specified")
		}
	}
	// Reject certificate or key with loopback default
	if opt.Listen == "" {
		if opt.TLSCert != "" {
			return fmt.Errorf("server certificate specified, but no listen address")
		}
		if opt.TLSKey != "" {
			return fmt.Errorf("server key specified, but no listen address")
		}
	}
	// Reject certificate or key with TLS disabled
	if opt.NoTLS {
		if opt.TLSCert != "" {
			return fmt.Errorf("server certificate specified while disabling TLS")
		}
		if opt.TLSKey != "" {
			return fmt.Errorf("server key specified while disabling TLS")
		}
	}
	// Reject disabling TLS when using loopback
	if opt.NoTLS && opt.Listen == "" {
		return fmt.Errorf("disabling TLS is not needed for loopback")
	}
	return nil
}

func fatal(err error, logf, csvlogf *os.File) error {
	if logf != nil {
		_ = logf.Close()
	}
	if csvlogf != nil {
		_ = csvlogf.Close()
	}
	return fmt.Errorf("server stopped: %s", err)
}

func initColor() error {
	switch colorMode {
	case "always":
		color.AlwaysColor()
	case "auto":
		color.AutoColor()
	default:
		color.NeverColor()
	}
	colorInitialized = true
	return nil
}
