package reset

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Reset(opt *option.Reset) error {
	// Validate options
	if !strings.HasPrefix(opt.Connector, "db.") {
		return fmt.Errorf("invalid database connector: %s", opt.Connector)
	}
	if !opt.Force {
		// Ask for confirmation
		_, _ = fmt.Fprintf(os.Stderr, "metadb: reset current data in %q? ", opt.Connector)
		var confirm string
		_, err := fmt.Scanln(&confirm)
		if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
			return nil
		}
	}
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
	// Disable source connectors before beginning reset
	/*
		err = sysdb.DisableSourceConnectors()
		if err != nil {
			return fmt.Errorf("disabling source connectors: %s", err)
		}
	*/
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
		eout.Info("resetting: %s", t.String())
		q := "UPDATE " + t.Id(db.Type) + " SET __cf=FALSE WHERE __cf AND __origin IN (" + origins + ")"
		_, err := db.ExecContext(context.TODO(), q)
		if err != nil {
			return err
		}
		q = "UPDATE " + t.History().Id(db.Type) + " SET __cf=FALSE WHERE __cf AND __current AND __origin IN (" + origins + ")"
		_, err = db.ExecContext(context.TODO(), q)
		if err != nil {
			return err
		}
		if err = sqlx.VacuumAnalyze(db, &t); err != nil {
			return err
		}
	}
	eout.Info("completed reset")
	return nil
}
