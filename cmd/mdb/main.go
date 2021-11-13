package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/metadb-project/metadb/cmd/internal/color"
	"github.com/metadb-project/metadb/cmd/internal/common"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/mdb/config"
	"github.com/metadb-project/metadb/cmd/mdb/disable"
	"github.com/metadb-project/metadb/cmd/mdb/enable"
	"github.com/metadb-project/metadb/cmd/mdb/option"
	"github.com/metadb-project/metadb/cmd/mdb/status"
	"github.com/metadb-project/metadb/cmd/mdb/user"
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
	devMode = os.Getenv("METADB_DEV") == "on"
	mdbMain()
}

func mdbMain() {
	// Initialize error output
	eout.Init(program)
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
	var configOpt = option.Config{}
	var userOpt = option.User{}
	var statusOpt = option.Status{}
	var enableOpt = option.Enable{}
	var disableOpt = option.Disable{}

	var passwordPrompt bool

	var cmdConfig = &cobra.Command{
		Use:  "config",
		Args: cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			if err = validateGlobalOptions(&globalOpt); err != nil {
				return err
			}
			configOpt.Global = globalOpt
			if len(args) > 0 {
				configOpt.Attr = &args[0]
			}
			if len(args) > 1 {
				configOpt.Val = &args[1]
			}
			if configOpt.Val != nil && strings.HasPrefix(*configOpt.Val, "@") {
				var valFile = strings.TrimPrefix(*configOpt.Val, "@")
				if *configOpt.Val, err = ValueFromFile(valFile); err != nil {
					return err
				}
			}
			if passwordPrompt {
				if configOpt.Attr == nil {
					return fmt.Errorf("attribute not specified")
				}
				if configOpt.Val != nil {
					return fmt.Errorf("password prompt and value cannot both be specified")
				}
				if !strings.HasSuffix(*configOpt.Attr, "password") {
					return fmt.Errorf("prompt is only valid for passwords")
				}
				var pw string
				if pw, err = inputPassword("Password for \""+(*configOpt.Attr)+"\": ", false); err != nil {
					return err
				}
				configOpt.Val = &pw
			}
			if err = config.Config(&configOpt); err != nil {
				return err
			}
			return nil
		},
	}
	cmdConfig.SetHelpFunc(help)
	cmdConfig.Flags().BoolVarP(&passwordPrompt, "pwprompt", "P", false, "")
	cmdConfig.Flags().BoolVarP(&configOpt.Delete, "delete", "d", false, "")
	cmdConfig.Flags().BoolVarP(&configOpt.List, "list", "l", false, "")
	_ = hostFlag(cmdConfig, &globalOpt.Host)
	_ = adminPortFlag(cmdConfig, &globalOpt.AdminPort)
	_ = verboseFlag(cmdConfig, &eout.EnableVerbose)
	_ = traceFlag(cmdConfig, &eout.EnableTrace)
	_ = noTLSFlag(cmdConfig, &globalOpt.NoTLS)
	_ = skipVerifyFlag(cmdConfig, &globalOpt.TLSSkipVerify)

	var cmdUser = &cobra.Command{
		Use:  "user",
		Args: cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = initColor(); err != nil {
				return err
			}
			if err = validateGlobalOptions(&globalOpt); err != nil {
				return err
			}
			userOpt.Global = globalOpt
			if len(args) > 0 {
				userOpt.Name = &args[0]
			}
			if len(args) > 1 {
				userOpt.Tables = &args[1]
			}
			if userOpt.Create {
				var pw string
				if pw, err = inputPassword("Password for \""+(*userOpt.Name)+"\": ", false); err != nil {
					return err
				}
				userOpt.Password = pw
			}
			if err = user.User(&userOpt); err != nil {
				return err
			}
			return nil
		},
	}
	cmdUser.SetHelpFunc(help)
	cmdUser.Flags().BoolVarP(&userOpt.Create, "create", "c", false, "")
	cmdUser.Flags().BoolVarP(&userOpt.Delete, "delete", "d", false, "")
	cmdUser.Flags().BoolVarP(&userOpt.List, "list", "l", false, "")
	_ = hostFlag(cmdUser, &globalOpt.Host)
	_ = adminPortFlag(cmdUser, &globalOpt.AdminPort)
	_ = verboseFlag(cmdUser, &eout.EnableVerbose)
	_ = traceFlag(cmdUser, &eout.EnableTrace)
	_ = noTLSFlag(cmdUser, &globalOpt.NoTLS)
	_ = skipVerifyFlag(cmdUser, &globalOpt.TLSSkipVerify)

	var cmdEnable = &cobra.Command{
		Use:  "enable",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if err = initColor(); err != nil {
				return
			}
			if err = validateGlobalOptions(&globalOpt); err != nil {
				return
			}
			enableOpt.Global = globalOpt
			enableOpt.Connectors = args
			if err = enable.Enable(&enableOpt); err != nil {
				return
			}
			err = nil
			return
		},
	}
	cmdEnable.SetHelpFunc(help)
	_ = hostFlag(cmdEnable, &globalOpt.Host)
	_ = adminPortFlag(cmdEnable, &globalOpt.AdminPort)
	_ = verboseFlag(cmdEnable, &eout.EnableVerbose)
	_ = traceFlag(cmdEnable, &eout.EnableTrace)
	_ = noTLSFlag(cmdEnable, &globalOpt.NoTLS)
	_ = skipVerifyFlag(cmdEnable, &globalOpt.TLSSkipVerify)

	var cmdDisable = &cobra.Command{
		Use:  "disable",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if err = initColor(); err != nil {
				return
			}
			if err = validateGlobalOptions(&globalOpt); err != nil {
				return
			}
			disableOpt.Global = globalOpt
			disableOpt.Connectors = args
			if err = disable.Disable(&disableOpt); err != nil {
				return
			}
			err = nil
			return
		},
	}
	cmdDisable.SetHelpFunc(help)
	_ = hostFlag(cmdDisable, &globalOpt.Host)
	_ = adminPortFlag(cmdDisable, &globalOpt.AdminPort)
	_ = verboseFlag(cmdDisable, &eout.EnableVerbose)
	_ = traceFlag(cmdDisable, &eout.EnableTrace)
	_ = noTLSFlag(cmdDisable, &globalOpt.NoTLS)
	_ = skipVerifyFlag(cmdDisable, &globalOpt.TLSSkipVerify)

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
			if err = status.Status(&statusOpt); err != nil {
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
	// Redefine help flag without -h; so we can use it for something else.
	var helpFlag bool
	rootCmd.PersistentFlags().BoolVarP(&helpFlag, "help", "", false, "Help for mdb")
	// Add commands.
	rootCmd.AddCommand(cmdConfig, cmdUser, cmdEnable, cmdDisable, cmdStatus, cmdVersion, cmdCompletion)
	var err error
	if err = rootCmd.Execute(); err != nil {
		return err
	}

	return nil
}

var helpConfig = "Configure or show server settings\n"
var helpUser = "Configure or show database user permissions\n"
var helpEnable = "Enable database or source connectors\n"
var helpDisable = "Disable database or source connectors\n"
var helpStatus = "Print server status\n"
var helpVersion = "Print mdb version\n"
var helpCompletion = "Generate command-line completion\n"

func help(cmd *cobra.Command, commandLine []string) {
	_ = commandLine
	switch cmd.Use {
	case "mdb":
		fmt.Printf("" +
			"Metadb client\n" +
			"\n" +
			"Usage:  mdb <command> <arguments>\n" +
			"\n" +
			"Commands:\n" +
			"  config                      - " + helpConfig +
			"  user                        - " + helpUser +
			"  enable                      - " + helpEnable +
			"  disable                     - " + helpDisable +
			//"  status                      - " + helpStatus +
			"  version                     - " + helpVersion +
			"  completion                  - " + helpCompletion +
			"\n" +
			"Use \"mdb help <command>\" for more information about a command.\n")
	case "config":
		fmt.Printf("" +
			helpConfig +
			"\n" +
			"Usage:  mdb config [<options>] [<attribute> [<value>]]\n" +
			"\n" +
			"Options:\n" +
			"  -P, --pwprompt              - Prompt for password value\n" +
			"  -d, --delete                - Delete value for specified attribute\n" +
			"  -l, --list                  - List all attributes and values\n" +
			hostFlag(nil, nil) +
			adminPortFlag(nil, nil) +
			verboseFlag(nil, nil) +
			noTLSFlag(nil, nil) +
			skipVerifyFlag(nil, nil) +
			traceFlag(nil, nil) +
			"\n" +
			"Database connector attributes:\n" +
			"  db.<name>.type              - Type of database system (must be set to\n" +
			"                                \"postgresql\")\n" +
			//"  db.<name>.type              - Type of database system: \"postgresql\" or\n" +
			//"                                \"redshift\"\n" +
			"  db.<name>.host              - Host name of the database server\n" +
			"  db.<name>.port              - TCP port the database server listens on\n" +
			"  db.<name>.dbname            - Name of the database\n" +
			"  db.<name>.adminuser         - Database user that owns the database\n" +
			"  db.<name>.adminpassword     - Password of adminuser\n" +
			"  db.<name>.superuser         - Database superuser\n" +
			"  db.<name>.superpassword     - Password of superuser\n" +
			"  db.<name>.sslmode           - SSL mode for connection to database (default:\n" +
			"                                \"require\")\n" +
			"\n" +
			"Source connector attributes:\n" +
			"  src.<name>.brokers          - Kafka bootstrap servers (comma-separated list)\n" +
			"  src.<name>.security         - Security protocol: \"ssl\" or \"plaintext\"\n" +
			"                                (default: \"ssl\")\n" +
			"  src.<name>.topics           - Regular expressions matching Kafka topics to\n" +
			"                                read (comma-separated list)\n" +
			"  src.<name>.group            - Kafka consumer group ID\n" +
			"  src.<name>.schemapassfilter - Regular expressions matching schema names to\n" +
			"                                accept (comma-separated list)\n" +
			"  src.<name>.schemaprefix     - Prefix to add to schema names\n" +
			"  src.<name>.dbs              - Names of database connectors to associate with\n" +
			"                                this source, where data will be written\n" +
			"                                (comma-separated list)\n" +
			"\n" +
			"Use @<file> in place of <value> to read the value from a file.\n" +
			"")
	case "user":
		fmt.Printf("" +
			helpUser +
			"\n" +
			"Usage:  mdb user [<options>] [<username> [<tables>]]\n" +
			"\n" +
			"Options:\n" +
			"  -c, --create                - Create database user\n" +
			"  -d, --delete                - Delete specified user permissions\n" +
			"  -l, --list                  - List all user permissions\n" +
			hostFlag(nil, nil) +
			adminPortFlag(nil, nil) +
			verboseFlag(nil, nil) +
			noTLSFlag(nil, nil) +
			skipVerifyFlag(nil, nil) +
			traceFlag(nil, nil) +
			"")
	case "enable":
		fmt.Printf("" +
			helpConfig +
			"\n" +
			"Usage:  mdb enable [<options>] <connector>...\n" +
			"\n" +
			"Options:\n" +
			hostFlag(nil, nil) +
			adminPortFlag(nil, nil) +
			verboseFlag(nil, nil) +
			noTLSFlag(nil, nil) +
			skipVerifyFlag(nil, nil) +
			traceFlag(nil, nil) +
			"\n" +
			"Connectors:\n" +
			"  db.<name>\n" +
			"  src.<name>\n" +
			"")
	case "disable":
		fmt.Printf("" +
			helpConfig +
			"\n" +
			"Usage:  mdb disable [<options>] <connector>...\n" +
			"\n" +
			"Options:\n" +
			hostFlag(nil, nil) +
			adminPortFlag(nil, nil) +
			verboseFlag(nil, nil) +
			noTLSFlag(nil, nil) +
			skipVerifyFlag(nil, nil) +
			traceFlag(nil, nil) +
			"\n" +
			"Connectors:\n" +
			"  db.<name>\n" +
			"  src.<name>\n" +
			"")
	case "status":
		fmt.Printf("" +
			helpStatus +
			"\n" +
			"Usage:  mdb status\n")
	case "version":
		fmt.Printf("" +
			helpVersion +
			"\n" +
			"Usage:  mdb version\n")
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
		"  -v, --verbose               - Enable verbose output\n"
}

func traceFlag(cmd *cobra.Command, trace *bool) string {
	if devMode {
		if cmd != nil {
			cmd.Flags().BoolVar(trace, "xtrace", false, "")
		}
		return "" +
			"      --xtrace                - Enable extremely verbose output\n"
	}
	return ""
}

func skipVerifyFlag(cmd *cobra.Command, skipVerify *bool) string {
	if cmd != nil {
		cmd.Flags().BoolVar(skipVerify, "skipverify", false, "")
	}
	return "" +
		"      --skipverify            - Do not verify server certificate chain and host\n" +
		"                                name [insecure, use for testing only]\n"
}

func noTLSFlag(cmd *cobra.Command, noTLS *bool) string {
	if cmd != nil {
		cmd.Flags().BoolVar(noTLS, "notls", false, "")
	}
	return "" +
		"      --notls                 - Disable TLS in connection to Metadb server\n" +
		"                                [insecure, use for testing only]\n"
}

func hostFlag(cmd *cobra.Command, host *string) string {
	if cmd != nil {
		cmd.Flags().StringVarP(host, "host", "h", "", "")
	}
	return "" +
		"  -h, --host <h>              - Metadb server host (default: 127.0.0.1)\n"
}

func adminPortFlag(cmd *cobra.Command, adminPort *string) string {
	if cmd != nil {
		cmd.Flags().StringVar(adminPort, "adminport", common.DefaultAdminPort, "")
	}
	return "" +
		"      --adminport <p>         - Metadb server admin port (default: " + common.DefaultAdminPort + ")\n"
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

func ValueFromFile(filename string) (string, error) {
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
