package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/metadb-project/metadb/cmd/internal/color"
	"github.com/metadb-project/metadb/cmd/internal/common"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/mdb/config"
	"github.com/metadb-project/metadb/cmd/mdb/option"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var program = "mdb"

// metadbVersion is defined at build time via -ldflags.
var metadbVersion string = "(unknown version)"

var colorMode string
var devMode bool

var colorInitialized bool

func main() {
	colorMode = os.Getenv("METADB_COLOR")
	devMode = (os.Getenv("METADB_DEV") == "on")
	mdbMain()
}

func mdbMain() {
	// Initialize error output
	eout.Init(program)
	// Run
	var err error
	if err = run(os.Args); err != nil {
		if !colorInitialized {
			color.NeverColor()
		}
		eout.Error("%s", err)
		os.Exit(1)
	}
}

func run(args []string) error {

	var globalOpt = option.Global{}
	var configDatabaseOpt = option.ConfigDatabase{}
	var configSourceOpt = option.ConfigSource{}
	var statusOpt = option.Status{}

	var passwordFile string
	var passwordPrompt bool

	var cmdConfigDatabase = &cobra.Command{
		Use: "config-database",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			if err = validateGlobalOptions(&globalOpt); err != nil {
				return err
			}
			configDatabaseOpt.Global = globalOpt
			if passwordFile != "" && passwordPrompt {
				return fmt.Errorf("password file and prompt cannot be used together")
			}
			if passwordFile != "" {
				if configDatabaseOpt.DBPassword, err = ReadPasswordFile(passwordFile); err != nil {
					return err
				}
			}
			if passwordPrompt {
				if configDatabaseOpt.DBPassword, err = inputPassword("Password for database user "+configDatabaseOpt.DBUser+": ", false); err != nil {
					return err
				}
			}
			if err = config.ConfigDatabase(&configDatabaseOpt); err != nil {
				return err
			}
			return nil
		},
	}
	cmdConfigDatabase.SetHelpFunc(help)
	cmdConfigDatabase.Flags().StringVar(&configDatabaseOpt.Name, "name", "", "")
	cmdConfigDatabase.Flags().StringVar(&configDatabaseOpt.Type, "type", "", "")
	cmdConfigDatabase.Flags().StringVar(&configDatabaseOpt.DBHost, "dbhost", "", "")
	cmdConfigDatabase.Flags().StringVar(&configDatabaseOpt.DBPort, "dbport", "", "")
	cmdConfigDatabase.Flags().StringVar(&configDatabaseOpt.DBName, "dbname", "", "")
	cmdConfigDatabase.Flags().StringVar(&configDatabaseOpt.DBUser, "dbuser", "", "")
	cmdConfigDatabase.Flags().BoolVar(&passwordPrompt, "dbpwprompt", false, "")
	cmdConfigDatabase.Flags().StringVar(&passwordFile, "dbpwfile", "", "")
	cmdConfigDatabase.Flags().StringVar(&configDatabaseOpt.DBSSLMode, "dbsslmode", "require", "")
	_ = hostFlag(cmdConfigDatabase, &globalOpt.Host)
	_ = adminPortFlag(cmdConfigDatabase, &globalOpt.AdminPort)
	_ = verboseFlag(cmdConfigDatabase, &eout.EnableVerbose)
	_ = traceFlag(cmdConfigDatabase, &eout.EnableTrace)
	_ = noTLSFlag(cmdConfigDatabase, &globalOpt.NoTLS)
	_ = skipVerifyFlag(cmdConfigDatabase, &globalOpt.TLSSkipVerify)

	var cmdConfigSource = &cobra.Command{
		Use: "config-source",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			if err = validateGlobalOptions(&globalOpt); err != nil {
				return err
			}
			configSourceOpt.Global = globalOpt
			if err = config.ConfigSource(&configSourceOpt); err != nil {
				return err
			}
			return nil
		},
	}
	cmdConfigSource.SetHelpFunc(help)
	cmdConfigSource.Flags().StringVar(&configSourceOpt.Name, "name", "", "")
	cmdConfigSource.Flags().StringVar(&configSourceOpt.Brokers, "brokers", "", "")
	cmdConfigSource.Flags().StringArrayVar(&configSourceOpt.Topics, "topic", []string{}, "")
	cmdConfigSource.Flags().StringVar(&configSourceOpt.Group, "group", "", "")
	cmdConfigSource.Flags().StringArrayVar(&configSourceOpt.SchemaPassFilter, "schema-pass-filter", []string{}, "")
	cmdConfigSource.Flags().StringVar(&configSourceOpt.SchemaPrefix, "schema-prefix", "", "")
	cmdConfigSource.Flags().StringArrayVar(&configSourceOpt.Databases, "database", []string{}, "")
	_ = hostFlag(cmdConfigSource, &globalOpt.Host)
	_ = adminPortFlag(cmdConfigSource, &globalOpt.AdminPort)
	_ = verboseFlag(cmdConfigSource, &eout.EnableVerbose)
	_ = traceFlag(cmdConfigSource, &eout.EnableTrace)
	_ = noTLSFlag(cmdConfigSource, &globalOpt.NoTLS)
	_ = skipVerifyFlag(cmdConfigSource, &globalOpt.TLSSkipVerify)

	var cmdStatus = &cobra.Command{
		Use: "status",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			if err = validateGlobalOptions(&globalOpt); err != nil {
				return err
			}
			statusOpt.Global = globalOpt
			if err = config.Status(&statusOpt); err != nil {
				return err
			}
			return nil
		},
	}
	cmdStatus.SetHelpFunc(help)
	_ = hostFlag(cmdStatus, &globalOpt.Host)
	_ = adminPortFlag(cmdStatus, &globalOpt.AdminPort)
	_ = verboseFlag(cmdStatus, &eout.EnableVerbose)
	_ = traceFlag(cmdStatus, &eout.EnableTrace)
	_ = noTLSFlag(cmdStatus, &globalOpt.NoTLS)
	_ = skipVerifyFlag(cmdStatus, &globalOpt.TLSSkipVerify)

	var cmdVersion = &cobra.Command{
		Use: "version",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("mdb version %s\n", metadbVersion)
			return nil
		},
	}
	cmdVersion.SetHelpFunc(help)

	var cmdCompletion = &cobra.Command{
		Use:                   "completion",
		DisableFlagsInUseLine: true,
		ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
		Args:                  cobra.ExactValidArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			switch args[0] {
			case "bash":
				if err = cmd.Root().GenBashCompletion(os.Stdout); err != nil {
					return err
				}
			case "zsh":
				if err = cmd.Root().GenZshCompletion(os.Stdout); err != nil {
					return err
				}
			case "fish":
				if err = cmd.Root().GenFishCompletion(os.Stdout, true); err != nil {
					return err
				}
			case "powershell":
				if err = cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmdCompletion.SetHelpFunc(help)

	var rootCmd = &cobra.Command{
		Use:                "mdb",
		SilenceErrors:      true,
		SilenceUsage:       true,
		DisableSuggestions: true,
	}
	rootCmd.SetHelpFunc(help)
	// Redefine help flag without -h; so we can use it for
	// something else.
	var helpFlag bool
	rootCmd.PersistentFlags().BoolVarP(&helpFlag, "help", "", false, "Help for mdb")
	// Add commands.
	rootCmd.AddCommand(cmdConfigDatabase, cmdConfigSource, cmdStatus, cmdVersion, cmdCompletion)
	var err error
	if err = rootCmd.Execute(); err != nil {
		return err
	}

	return nil
}

var helpConfigDatabase = "Configure database connector\n"
var helpConfigSource = "Configure data source connector\n"
var helpStatus = "Print server status\n"
var helpVersion = "Print mdb version\n"
var helpCompletion = "Generate command-line completion\n"

func help(cmd *cobra.Command, commandLine []string) {
	switch cmd.Use {
	case "mdb":
		fmt.Printf("" +
			"Metadb client\n" +
			"\n" +
			"Usage:  mdb <command> [<flags>]\n" +
			"\n" +
			"Commands:\n" +
			"  config-database             - " + helpConfigDatabase +
			"  config-source               - " + helpConfigSource +
			"  status                      - " + helpStatus +
			"  version                     - " + helpVersion +
			"  completion                  - " + helpCompletion +
			"\n" +
			"Use \"mdb help <command>\" for more information about a command.\n")
	case "config-database":
		fmt.Printf("" +
			helpConfigDatabase +
			"\n" +
			"Usage:  mdb config-database <flags>\n" +
			"\n" +
			"Options:\n" +
			"    --name <n>                - Name of the database connector\n" +
			"    --type <t>                - Type of database system: \"postgresql\" or\n" +
			"                                \"redshift\" (default \"postgresql\")\n" +
			"    --dbhost <h>              - Host name of the database server\n" +
			"    --dbport <p>              - TCP port the database server listens on\n" +
			"    --dbname <d>              - Name of the database\n" +
			"    --dbuser <u>              - User that owns the database\n" +
			"    --dbpwprompt              - Prompt for user password\n" +
			"    --dbpwfile <f>            - File to read user password from\n" +
			"    --dbsslmode <m>           - SSL mode for connection to database (default:\n" +
			"                                \"require\")\n" +
			hostFlag(nil, nil) +
			adminPortFlag(nil, nil) +
			verboseFlag(nil, nil) +
			noTLSFlag(nil, nil) +
			skipVerifyFlag(nil, nil) +
			traceFlag(nil, nil) +
			"")
	case "config-source":
		fmt.Printf("" +
			helpConfigSource +
			"\n" +
			"Usage:  mdb config-source <flags>\n" +
			"\n" +
			"Options:\n" +
			"    --name <n>                - Name of the data source connector\n" +
			"    --brokers <b1>[,<b2>...]  - Kafka bootstrap servers\n" +
			"    --topic <t>               - Regular expression matching Kafka topics to\n" +
			"                                read; this flag may be repeated for multiple\n" +
			"                                expressions\n" +
			"    --group <g>               - Kafka consumer group ID\n" +
			"    --schema-pass-filter <f>  - Regular expression matching schema names to\n" +
			"                                accept; this flag may be repeated for multiple\n" +
			"                                expressions\n" +
			"    --schema-prefix <p>       - Prefix to add to schema names\n" +
			"    --database <d>            - Name of a database connector to associate with\n" +
			"                                this source, where data will be written\n" +
			hostFlag(nil, nil) +
			adminPortFlag(nil, nil) +
			verboseFlag(nil, nil) +
			noTLSFlag(nil, nil) +
			skipVerifyFlag(nil, nil) +
			traceFlag(nil, nil) +
			"")
	case "version":
		fmt.Printf("" +
			helpVersion +
			"\n" +
			"Usage:  mdb version\n")
	case "status":
		fmt.Printf("" +
			helpStatus +
			"\n" +
			"Usage:  mdb status\n")
	case "completion":
		fmt.Printf("" +
			helpCompletion +
			"\n" +
			"Usage:  mdb completion <shell>\n" +
			"\n" +
			"Shells:\n" +
			"  bash\n" +
			"  zsh\n" +
			"  fish\n" +
			"  powershell\n")
	default:
	}
}

func verboseFlag(cmd *cobra.Command, verbose *bool) string {
	if cmd != nil {
		cmd.Flags().BoolVarP(verbose, "verbose", "v", false, "")
	}
	return "" +
		"-v, --verbose                 - Enable verbose output\n"
}

func traceFlag(cmd *cobra.Command, trace *bool) string {
	if devMode {
		if cmd != nil {
			cmd.Flags().BoolVar(trace, "xtrace", false, "")
		}
		return "" +
			"    --xtrace                  - Enable extremely verbose output\n"
	}
	return ""
}

func skipVerifyFlag(cmd *cobra.Command, skipVerify *bool) string {
	if cmd != nil {
		cmd.Flags().BoolVar(skipVerify, "skip-verify", false, "")
	}
	return "" +
		"    --skip-verify             - Do not verify server certificate chain and host\n" +
		"                                name [insecure, use for testing only]\n"
}

func noTLSFlag(cmd *cobra.Command, noTLS *bool) string {
	if cmd != nil {
		cmd.Flags().BoolVar(noTLS, "no-tls", false, "")
	}
	return "" +
		"    --no-tls                  - Disable TLS in connection to Metadb server\n" +
		"                                [insecure, use for testing only]\n"
}

func hostFlag(cmd *cobra.Command, host *string) string {
	if cmd != nil {
		cmd.Flags().StringVarP(host, "host", "h", "", "")
	}
	return "" +
		"-h, --host <h>                - Metadb server host (default: 127.0.0.1)\n"
}

func adminPortFlag(cmd *cobra.Command, adminPort *string) string {
	if cmd != nil {
		cmd.Flags().StringVar(adminPort, "admin-port", common.DefaultAdminPort, "")
	}
	return "" +
		"    --admin-port <p>          - Metadb server admin port (default: " + common.DefaultAdminPort + ")\n"
}

func validateGlobalOptions(opt *option.Global) error {
	if opt.Host == "" && opt.NoTLS {
		return fmt.Errorf("disabling TLS is not needed for loopback")
	}
	return nil
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

func ReadPasswordFile(filename string) (string, error) {
	var err error
	var f *os.File
	if f, err = os.Open(filename); err != nil {
		return "", err
	}
	var scanner = bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords)
	var ok bool
	if ok = scanner.Scan(); !ok {
		if scanner.Err() != nil {
			return "", err
		}
	}
	return scanner.Text(), nil
}

// inputPassword gets keyboard input from the user with terminal echo disabled.
// This function is intended for inputting passwords.  It prints a specified
// prompt before the input, and can optionally input the password a second time
// for confirmation.  The password is returned, or an error if there was a
// problem reading the input or (in the case of a confirmation input) if the
// two passwords did not match.  SIGINT is disabled during the input, to avoid
// leaving the terminal in a no-echo state.
func inputPassword(prompt string, confirm bool) (string, error) {
	// Ignore SIGINT, to avoid leaving terminal in no-echo state.
	signal.Ignore(os.Interrupt)
	defer signal.Reset(os.Interrupt)
	// Read the input.
	fmt.Print(prompt)
	p, err := term.ReadPassword(syscall.Stdin)
	fmt.Println("")
	if err != nil {
		return "", err
	}
	// Read the input again to confirm.
	if confirm {
		fmt.Print("(Confirming) " + prompt)
		q, err := term.ReadPassword(syscall.Stdin)
		fmt.Println("")
		if err != nil {
			return "", err
		}
		if string(p) != string(q) {
			return "", errors.New("passwords do not match")
		}
	}
	// Return password.
	return string(p), nil
}
