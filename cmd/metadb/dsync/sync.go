package dsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

type Mode int

const (
	NoSync      Mode = 0
	InitialSync Mode = 1
	Resync      Mode = 2
)

var _ = InitialSync

func SetSyncMode(dq dbx.Queryable, mode Mode, source string) error {
	q := "UPDATE metadb.source SET sync=$1 WHERE name=$2"
	if _, err := dq.Exec(context.TODO(), q, int16(mode), source); err != nil {
		return err
	}
	return nil
}

func ReadSyncMode(dq dbx.Queryable, source string) (Mode, error) {
	var syncMode int16
	q := "SELECT sync FROM metadb.source WHERE name=$1"
	err := dq.QueryRow(context.TODO(), q, source).Scan(&syncMode)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, fmt.Errorf("unable to query sync mode")
	case err != nil:
		return 0, fmt.Errorf("querying sync mode: %s", err)
	default:
		return Mode(syncMode), nil
	}
}

func Sync(opt *option.Sync) error {
	db, err := util.ReadConfigDatabase(opt.Datadir)
	if err != nil {
		return err
	}
	dp, err := dbx.NewPool(context.TODO(), db.ConnString(db.User, db.Password))
	if err != nil {
		return fmt.Errorf("creating database connection pool: %w", err)
	}
	defer dp.Close()
	exists, err := sourceExists(dp, opt.Source)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("data source %q does not exist", opt.Source)
	}
	syncMode, err := ReadSyncMode(dp, opt.Source)
	if err != nil {
		return err
	}
	if syncMode != NoSync {
		fmt.Fprintf(os.Stderr, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
		fmt.Fprintf(os.Stderr, "WARNING: Synchronization in progress for data source %q.\n", opt.Source)
		fmt.Fprintf(os.Stderr, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
		fmt.Fprintf(os.Stderr, "Interrupt and restart synchronization with new snapshot? ")
		var confirm string
		_, err = fmt.Scanln(&confirm)
		if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
			return nil
		}
	}
	if !opt.Force {
		// Ask for confirmation
		_, _ = fmt.Fprintf(os.Stderr, "Begin synchronization process for data source %q? ", opt.Source)
		var confirm string
		_, err = fmt.Scanln(&confirm)
		if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
			return nil
		}
	}

	// Check if server is already running.
	running, pid, err := process.IsServerRunning(opt.Datadir)
	if err != nil {
		return err
	}
	if running {
		return fmt.Errorf("lock file %q already exists and server (PID %d) appears to be running", util.SystemPIDFileName(opt.Datadir), pid)
	}
	// Write lock file for new server instance.
	if err = process.WritePIDFile(opt.Datadir); err != nil {
		return err
	}
	defer process.RemovePIDFile(opt.Datadir)

	// Check that database version is compatible.
	if err = catalog.CheckDatabaseCompatible(dp); err != nil {
		return err
	}

	//log.Init(ioutil.Discard, false, false)
	//log.SetDatabase(dp)
	//log.Info("resync started")
	// Get list of tables
	cat, err := catalog.Initialize(db, dp)
	if err != nil {
		return err
	}
	tables := cat.AllTables(opt.Source)
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].String() < tables[j].String()
	})
	eout.Info("sync: preparing tables for new snapshot")
	for _, t := range tables {
		synct := catalog.SyncTable(&t)
		synctsql := synct.SQL()
		q := "DROP INDEX IF EXISTS \"" + synct.Schema + "\".\"" + synct.Table + "___id_idx\""
		if _, err = dp.Exec(context.TODO(), q); err != nil {
			return err
		}
		q = "TRUNCATE " + synctsql
		if _, err = dp.Exec(context.TODO(), q); err != nil {
			return err
		}
	}
	if err = SetSyncMode(dp, Resync, opt.Source); err != nil {
		return err
	}
	eout.Info("sync: completed")
	return nil
}

func sourceExists(dq dbx.Queryable, sourceName string) (bool, error) {
	q := "SELECT 1 FROM metadb.source WHERE name=$1"
	var i int64
	err := dq.QueryRow(context.TODO(), q, sourceName).Scan(&i)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}
