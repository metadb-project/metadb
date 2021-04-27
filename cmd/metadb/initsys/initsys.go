package initsys

import (
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func InitSys(opt *option.Init) error {
	var err error
	// Require that a data directory be specified.
	if opt.Datadir == "" {
		return fmt.Errorf("data directory not specified")
	}
	// Require that the data directory not already exist.
	var exists bool
	if exists, err = util.FileExists(opt.Datadir); err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%s already exists", opt.Datadir)
	}
	eout.Info("initializing new instance in %s", opt.Datadir)
	// Create the data directory.
	eout.Verbose("creating data directory")
	eout.Trace("mkdir: %s", opt.Datadir)
	err = os.MkdirAll(opt.Datadir, util.ModePermRWX)
	if err != nil {
		return err
	}
	eout.Verbose("creating system files")
	if err = initSchema(opt.Datadir); err != nil {
		return err
	}
	eout.Verbose("initialization completed")
	return nil
}

func initSchema(datadir string) error {
	var err error
	// Create system directory.
	//var sdir = filepath.Join(datadir, sysdbDir)
	//eout.Trace("mkdir: %s", sdir)
	if err = os.MkdirAll(util.SystemDirName(datadir), util.ModePermRWX); err != nil {
		return err
	}
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
	if err = sysdb.InitCreate(util.SysdbFileName(datadir)); err != nil {
		return err
	}
	// TODO Do this as a function in sysdb - close, chmod, reopen
	/*
		db.Close()
		if err = os.Chmod(filename, util.ModePermRW); err != nil {
			return err
		}
	*/
	return nil
}
