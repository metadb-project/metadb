package initsys

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func InitSys(opt *option.Init) error {
	// Check for required options.
	if opt.Datadir == "" {
		return fmt.Errorf("data directory not specified")
	}

	// Require the database URI user to be postgres.
	/*
		var u *url.URL
		if u, err = url.Parse(opt.DatabaseURI); err != nil {
			return fmt.Errorf("parsing database connection URI: %v", err)
		}
		var username = u.User.Username()
		if username == "" {
			return fmt.Errorf("username not specified in database connection URI: %s", util.RedactPasswordInURI(u))
		}
		if username != "postgres" {
			return fmt.Errorf("unsupported username in database connection URI: %s", username)
		}
	*/
	/*
		dburi, err := dbx.NewDB(opt.DatabaseURI)
		if err != nil {
			return fmt.Errorf("parsing database URI: %s: %v", opt.DatabaseURI, err)
		}
		if dburi.User == "" {
			return fmt.Errorf("username not specified in database connection URI: %s", util.RedactPasswordInURI(opt.DatabaseURI))
		}
		if dburi.User != "postgres" {
			return fmt.Errorf("unsupported username in database connection URI: %s", dburi.User)
		}
	*/

	dd, err := filepath.Abs(opt.Datadir)
	if err != nil {
		return fmt.Errorf("absolute path: %v", err)
	}

	// Require that the data directory not already exist.
	exists, err := util.FileExists(dd)
	if err != nil {
		return fmt.Errorf("checking if path exists: %v", err)
	}
	if exists {
		return fmt.Errorf("%s already exists", dd)
	}

	// eout.Verbose("creating system files")
	// if err = initSchema(opt.Datadir, opt.DatabaseURI); err != nil {

	// Create system directory.
	//var sdir = filepath.Join(datadir, sysdbDir)
	//eout.Trace("mkdir: %s", sdir)

	// if err = os.MkdirAll(util.SystemDirName(datadir), util.ModePermRWX); err != nil {
	// 	return err
	// }

	//var filename = filepath.Join(sdir, sysdbFile)
	// Create and initialize system database.
	/*
		if err = sysdb.Init(filename); err != nil {
			return err
		}
	*/
	/*
		var db *sql.DB
		var dsn = sysdbPath + sqliteOptions
		eout.Trace("sysdb: dsn: %s", dsn)
		if db, err = sql.Open("sqlite3", dsn); err != nil {
			return err
		}
	*/
	/*
		// Set up schema.
		if err = sysdb.InitSchema(); err != nil {
			return err
		}
	*/

	// opt.DatabaseURI uses the postgres user; we need to create a metadb
	// user and use that for normal operations

	// Create the data directory.
	eout.Verbose("creating data directory")
	eout.Trace("mkdir: %s", dd)
	err = os.MkdirAll(dd, util.ModePermRWX)
	if err != nil {
		return fmt.Errorf("mkdir: %v", err)
	}

	eout.Verbose("writing configuration file")
	f, err := os.Create(util.ConfigFileName(dd))
	if err != nil {
		return fmt.Errorf("creating configuration file: %v", err)
	}
	var s = "[main]\n" +
		"host = \n" +
		"port = 5432\n" +
		"database = \n" +
		"superuser = postgres\n" +
		"superuser_password = \n" +
		"systemuser = mdbadmin\n" +
		"systemuser_password = \n" +
		"sslmode = require\n"
	_, err = f.WriteString(s)
	if err != nil {
		return fmt.Errorf("writing configuration file: %v", err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("closing configuration file: %v", err)
	}

	/*
		eout.Verbose("creating user")
		var systemUser = "metadb"
		var systemPassword string
		if systemPassword, err = util.GeneratePassword(); err != nil {
			return fmt.Errorf("generating password: %v", err)
		}
		if err = dbx.CreateUser(dburi, systemUser, systemPassword); err != nil {
			return err
		}
	*/

	/*
		eout.Verbose("writing system configuration file")
		if f, err = os.Create(util.SystemConfigFileName(opt.Datadir)); err != nil {
			return fmt.Errorf("creating system configuration file: %v", err)
		}
		s = "system_user = " + systemUser + "\nsystem_password = " + systemPassword + "\n"
		if _, err = f.WriteString(s); err != nil {
			return fmt.Errorf("writing system configuration file: %v", err)
		}
		if err = f.Close(); err != nil {
			return fmt.Errorf("closing system configuration file: %v", err)
		}
	*/

	/*
		eout.Verbose("creating database")
		if err = createDatabase(dburi, systemUser); err != nil {
			return fmt.Errorf("creating database: %v", err)
		}
	*/

	// var db *url.URL
	// if db, err = url.Parse(opt.DatabaseURI); err != nil {
	// 	return fmt.Errorf("parsing database connection URI: %s: %v", util.RedactPasswordInURI(db), err)
	// }
	/*
		db := dburi
		db.User = systemUser
		db.Password = systemPassword
	*/

	// if err = sysdb.InitCreate(opt.DatabaseURI); err != nil {
	// 	return fmt.Errorf("initializing database: %v", err)
	// }
	/*
		eout.Verbose("initializing database")
		if err = catalog.Initialize(db); err != nil {
			return fmt.Errorf("initializing database: %v", err)
		}
	*/

	// TODO Do this as a function in sysdb - close, chmod, reopen
	/*
		db.Close()
		if err = os.Chmod(filename, util.ModePermRW); err != nil {
			return err
		}
	*/

	eout.Info("initialized new data directory in %s", dd)
	return nil
}

func createDatabase(db *dbx.DB, owner string) error {
	dc, err := db.Connect()
	if err != nil {
		return err
	}
	defer dc.Close(context.TODO())

	_, err = dc.Exec(context.TODO(), "CREATE DATABASE "+db.DBName+" OWNER "+owner)
	if err != nil {
		return fmt.Errorf("%s: %v", db.DBName, err)
	}
	return nil
}
