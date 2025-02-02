package dsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metadb-project/metadb/cmd/metadb/eout"
	"github.com/metadb-project/metadb/cmd/metadb/tools"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func EndSync(opt *option.EndSync) error {
	var now = time.Now().UTC().Format(time.RFC3339)
	var db *dbx.DB
	var err error
	db, err = util.ReadConfigDatabase(opt.Datadir)
	if err != nil {
		return err
	}
	var dp *pgxpool.Pool
	dp, err = dbx.NewPool(context.TODO(), db.ConnString(db.User, db.Password))
	if err != nil {
		return fmt.Errorf("creating database connection pool: %w", err)
	}
	defer dp.Close()
	var exists bool
	exists, err = sourceExists(dp, opt.Source)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("data source %q does not exist", opt.Source)
	}
	// Continue only if we are in a sync mode.
	var syncMode Mode
	syncMode, err = ReadSyncMode(dp, opt.Source)
	if err != nil {
		return err
	}
	if syncMode == NoSync {
		return fmt.Errorf("\"endsync\" can only be used in sync mode")
	} // Allow initial sync or resync to continue.

	// Check if server is already running.
	var running bool
	var pid int
	running, pid, err = process.IsServerRunning(opt.Datadir)
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
	var cat *catalog.Catalog
	cat, err = catalog.Initialize(db, dp)
	if err != nil {
		return err
	}
	tables := cat.AllTables(opt.Source)
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].String() < tables[j].String()
	})
	if syncMode == Resync {
		// Before continuing, pause for confirmation if many records will be deleted.
		// Count sync table rows.
		eout.Info("endsync: safety check: reading sync record counts")
		var syncCount int64
		for _, t := range tables {
			synct := catalog.SyncTable(&t)
			var count int64
			q := "SELECT count(*) FROM " + synct.SQL()
			err = dp.QueryRow(context.TODO(), q).Scan(&count)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				return err
			case err != nil:
				return err
			default:
				syncCount += count
			}
		}
		// Count current records.
		eout.Info("endsync: safety check: reading current record counts")
		var currentCount int64
		for _, t := range tables {
			var count int64
			q := "SELECT count(*) FROM " + t.SQL()
			err = dp.QueryRow(context.TODO(), q).Scan(&count)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				return err
			case err != nil:
				return err
			default:
				currentCount += count
			}
		}
		// Calculate the approximate fraction of affected records.
		percent := (float64(currentCount) - float64(syncCount)) / float64(currentCount) * 100
		if percent > 100.0 {
			percent = 100.0
		}
		if percent > 20.0 {
			fmt.Fprintf(os.Stderr, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
			fmt.Fprintf(os.Stderr, "%.0f%% of current records have not been confirmed by the new snapshot.\n", percent)
			fmt.Fprintf(os.Stderr, "The unconfirmed records will be marked as deleted.\n")
			fmt.Fprintf(os.Stderr, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
			if !opt.ForceAll {
				fmt.Fprintf(os.Stderr, "Do you want to continue? ")
				var confirm string
				_, err = fmt.Scanln(&confirm)
				if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
					return nil
				}
			}
		}
		if !opt.Force && !opt.ForceAll {
			// Ask for confirmation
			_, _ = fmt.Fprintf(os.Stderr, "Finalize synchronization for data source %q? ", opt.Source)
			var confirm string
			_, err = fmt.Scanln(&confirm)
			if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
				return nil
			}
		}

		// Finalize tables.
		for _, t := range tables {
			eout.Info("endsync: finalizing table %s", t.String())
			synct := catalog.SyncTable(&t)
			synctsql := synct.SQL()
			q := "CREATE INDEX \"" + synct.Table + "___id_idx\" ON " + synctsql + "(__id)"
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
	}
	if syncMode == InitialSync {
		eout.Info("endsync: refreshing inferred column types")
		err = tools.RefreshInferredColumnTypes(dp, func(msg string) {
			eout.Info("endsync: %s", msg)
		})
		if err != nil {
			return err
		}
	}
	var tx pgx.Tx
	tx, err = dp.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	if syncMode == Resync {
		eout.Info("endsync: cleaning up sync data")
		for _, t := range tables {
			synct := catalog.SyncTable(&t)
			synctsql := synct.SQL()
			q := "DROP INDEX \"" + synct.Schema + "\".\"" + synct.Table + "___id_idx\""
			if _, err = tx.Exec(context.TODO(), q); err != nil {
				return err
			}
			q = "TRUNCATE " + synctsql
			if _, err = tx.Exec(context.TODO(), q); err != nil {
				return err
			}
		}
	}
	if err = SetSyncMode(tx, NoSync, opt.Source); err != nil {
		return err
	}
	if err = tx.Commit(context.TODO()); err != nil {
		return fmt.Errorf("committing changes: %w", err)
	}
	eout.Info("endsync: completed")
	// Sync marctab for full update and schedule maintenance.
	q := "UPDATE marctab.metadata SET version = 0"
	_, _ = dp.Exec(context.TODO(), q)
	q = "UPDATE metadb.maintenance SET next_maintenance_time = next_maintenance_time - interval '1 day'"
	if _, err = dp.Exec(context.TODO(), q); err != nil {
		return err
	}
	//log.Init(ioutil.Discard, false, false)
	//log.SetDatabase(dp)
	//log.Info("resync complete")
	return nil
}
