package clean

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Clean(opt *option.Clean) error {
	// Validate options
	if !strings.HasPrefix(opt.Connector, "db.") {
		return fmt.Errorf("invalid database connector: %s", opt.Connector)
	}
	// Ask for confirmation
	_, _ = fmt.Fprintf(os.Stderr, "metadb: remove old data in %q? ", opt.Connector)
	var confirm string
	_, err := fmt.Scanln(&confirm)
	if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	// Initialize sysdb
	if err := sysdb.Init(util.SysdbFileName(opt.Datadir)); err != nil {
		return fmt.Errorf("initializing system database: %s", err)
	}
	// Open database
	dbtype, dsn, err := sysdb.ReadDataSource(opt.Connector)
	if err != nil {
		return err
	}
	db, err := sqlx.Open(dbtype, dsn)
	if err != nil {
		return err
	}
	// Get list of tables
	tmap, err := metadata.TrackRead(*db)
	if err != nil {
		return err
	}
	var tables []sqlx.Table
	for t := range tmap {
		tables = append(tables, t)
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].String() < tables[j].String()
	})
	origins := sqlx.CSVToSQL(opt.Origins)
	for _, t := range tables {
		eout.Info("cleaning: %s", t.String())
		q := "DELETE FROM " + t.SQL() + " WHERE NOT __cf AND __origin IN (" + origins + ")"
		_, err := db.ExecContext(context.TODO(), q)
		if err != nil {
			return err
		}
		q = "UPDATE " + t.History().SQL() + " SET __cf=TRUE,__end='" + now + "',__current=FALSE WHERE NOT __cf AND __current AND __origin IN (" + origins + ")"
		_, err = db.ExecContext(context.TODO(), q)
		if err != nil {
			return err
		}
		// Any non-current historical data can be set to __cf=TRUE
		q = "UPDATE " + t.History().SQL() + " SET __cf=TRUE WHERE NOT __cf AND __origin IN (" + origins + ")"
		_, err = db.ExecContext(context.TODO(), q)
		if err != nil {
			return err
		}
		if err = sqlx.VacuumAnalyze(db, &t); err != nil {
			return err
		}
	}
	eout.Info("completed clean")
	return nil
}
