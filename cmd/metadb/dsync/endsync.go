package dsync

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func EndSync(opt *option.EndSync) error {
	// Validate options
	if !opt.Force {
		// Ask for confirmation
		_, _ = fmt.Fprintf(os.Stderr, "Remove unsynchronized data for data source %q? ", opt.Source)
		var confirm string
		_, err := fmt.Scanln(&confirm)
		if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
			return nil
		}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	db, err := util.ReadConfigDatabase(opt.Datadir)
	if err != nil {
		return err
	}
	dp, err := dbx.NewPool(context.TODO(), db.ConnString(db.User, db.Password))
	if err != nil {
		return fmt.Errorf("creating database connection pool: %v", err)
	}
	defer dp.Close()
	exists, err := sourceExists(dp, opt.Source)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("data source %q does not exist", opt.Source)
	}
	// Continue only if we are in a sync mode.
	syncMode, err := ReadSyncMode(dp, opt.Source)
	if err != nil {
		return err
	}
	if syncMode == NoSync {
		return fmt.Errorf("\"endsync\" can only be used in sync mode")
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

	// Get list of tables
	cat, err := catalog.Initialize(db, dp)
	if err != nil {
		return err
	}
	tables := cat.AllTables(opt.Source)
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].String() < tables[j].String()
	})
	for _, t := range tables {
		eout.Info("endsync: table %s", t.String())
		synct := SyncTable(&t)
		synctsql := synct.SQL()
		q := "CREATE INDEX \"" + synct.T + "___id_idx\" ON " + synctsql + "(__id)"
		if _, err = dp.Exec(context.TODO(), q); err != nil {
			return err
		}
		q = "UPDATE " + t.MainSQL() + " SET __end='" + now + "',__current='f' " +
			"WHERE __current AND" +
			" NOT EXISTS (SELECT __id FROM " + synctsql + " s WHERE " + t.MainSQL() + ".__id=s.__id)"
		if _, err = dp.Exec(context.TODO(), q); err != nil {
			return err
		}
	}
	eout.Info("endsync: cleaning up")
	tx, err := dp.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	for _, t := range tables {
		synct := SyncTable(&t)
		synctsql := synct.SQL()
		q := "DROP INDEX \"" + synct.S + "\".\"" + synct.T + "___id_idx\""
		if _, err = tx.Exec(context.TODO(), q); err != nil {
			return err
		}
		q = "TRUNCATE " + synctsql
		if _, err = tx.Exec(context.TODO(), q); err != nil {
			return err
		}
	}
	if err = SetSyncMode(tx, NoSync, opt.Source); err != nil {
		return err
	}
	if err = tx.Commit(context.TODO()); err != nil {
		return fmt.Errorf("committing changes: %v", err)
	}
	// Sync marctab for full update and schedule maintenance.
	q := "UPDATE marctab.metadata SET version = 0"
	_, _ = dp.Exec(context.TODO(), q)
	q = "UPDATE metadb.maintenance SET next_maintenance_time = next_maintenance_time - interval '1 day'"
	if _, err = dp.Exec(context.TODO(), q); err != nil {
		return err
	}
	eout.Info("completed")
	//log.Init(ioutil.Discard, false, false)
	//log.SetDatabase(dp)
	//log.Info("resync complete")
	return nil
}
