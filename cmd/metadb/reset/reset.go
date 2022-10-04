package reset

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Reset(opt *option.Reset) error {
	// Validate options
	if !strings.HasPrefix(opt.Connector, "db.") {
		return fmt.Errorf("invalid database connector: %s", opt.Connector)
	}
	if !opt.Force {
		// Ask for confirmation
		_, _ = fmt.Fprintf(os.Stderr, "Reset current data in %q? ", opt.Connector)
		var confirm string
		_, err := fmt.Scanln(&confirm)
		if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
			return nil
		}
	}
	// Initialize sysdb
	// if err := sysdb.Init(util.SysdbFileName(opt.Datadir)); err != nil {
	// 	return fmt.Errorf("initializing system database: %s", err)
	// }
	// Open database
	// dbtype, dsn, err := sysdb.ReadDataSource(opt.Connector)
	// if err != nil {
	// 	return err
	// }
	_, db, err := util.ReadConfigDatabase(opt.Datadir)
	if err != nil {
		return err
	}
	dsn := &sqlx.DSN{
		Host:     db.Host,
		Port:     "5432",
		User:     db.User,
		Password: db.Password,
		DBName:   db.DBName,
		SSLMode:  "require",
	}
	dc, err := sqlx.Open("postgres", dsn)
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
	tmap, err := metadata.TrackRead(dc)
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
		q := "UPDATE " + dc.TableSQL(&t) + " SET __cf=FALSE WHERE __cf AND __origin IN (" + origins + ")"
		if _, err = dc.Exec(nil, q); err != nil {
			return err
		}
		if err = dc.VacuumAnalyzeTable(&t); err != nil {
			return err
		}
		q = "UPDATE " + dc.HistoryTableSQL(&t) + " SET __cf=FALSE WHERE __cf AND __current AND __origin IN (" +
			origins + ")"
		if _, err = dc.Exec(nil, q); err != nil {
			return err
		}
		if err = dc.VacuumAnalyzeTable(dc.HistoryTable(&t)); err != nil {
			return err
		}
	}
	eout.Info("completed reset")
	return nil
}
