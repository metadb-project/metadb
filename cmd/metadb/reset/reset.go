package reset

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Reset(opt *option.Reset) error {
	// Validate options
	if !opt.Force {
		// Ask for confirmation
		_, _ = fmt.Fprintf(os.Stderr, "Reset current data for data source %q? ", opt.Source)
		var confirm string
		_, err := fmt.Scanln(&confirm)
		if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
			return nil
		}
	}
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
	// Disable source connectors before beginning reset
	/*
		err = sysdb.DisableSourceConnectors()
		if err != nil {
			return fmt.Errorf("disabling source connectors: %s", err)
		}
	*/
	if err = catalog.SetResyncMode(dp, true); err != nil {
		return err
	}
	log.Init(ioutil.Discard, false, false)
	log.SetDatabase(dp)
	log.Info("resync started")
	// Get list of tables
	cat, err := catalog.Initialize(db, dp)
	if err != nil {
		return err
	}
	tables := cat.AllTables()
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].String() < tables[j].String()
	})
	for _, t := range tables {
		eout.Info("resetting: %s", t.String())
		mainTable := t.MainSQL()
		q := "VACUUM ANALYZE " + mainTable
		if _, err := dp.Exec(context.TODO(), q); err != nil {
			return err
		}
		q = "UPDATE " + mainTable + " SET __cf=FALSE WHERE __cf AND __current AND " +
			"__source='" + opt.Source + "'"
		if _, err := dp.Exec(context.TODO(), q); err != nil {
			return err
		}
		q = "VACUUM ANALYZE " + mainTable
		if _, err := dp.Exec(context.TODO(), q); err != nil {
			return err
		}
	}
	eout.Info("completed reset")
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
