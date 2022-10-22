package clean

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Clean(opt *option.Clean) error {
	// Validate options
	if !opt.Force {
		// Ask for confirmation
		_, _ = fmt.Fprintf(os.Stderr, "Remove old data for data source %q? ", opt.Source)
		var confirm string
		_, err := fmt.Scanln(&confirm)
		if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
			return nil
		}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	// Initialize sysdb
	// if err := sysdb.Init(util.SysdbFileName(opt.Datadir)); err != nil {
	// 	return fmt.Errorf("initializing system database: %s", err)
	// }
	// Open database
	// dbtype, dsn, err := sysdb.ReadDataSource(opt.Connector)
	// if err != nil {
	// 	return err
	// }
	db, err := util.ReadConfigDatabase(opt.Datadir)
	if err != nil {
		return err
	}
	dsn := &sqlx.DSN{
		Host:     db.Host,
		Port:     db.Port,
		User:     db.User,
		Password: db.Password,
		DBName:   db.DBName,
		SSLMode:  db.SSLMode,
	}
	dc, err := sqlx.Open("postgresql", dsn)
	if err != nil {
		return err
	}
	defer dc.Close()
	exists, err := sourceExists(dc, opt.Source)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("data source %q does not exist", opt.Source)
	}
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
	for _, t := range tables {
		eout.Info("cleaning: %s", t.String())
		q := "DELETE FROM " + dc.TableSQL(&t) + " WHERE NOT __cf AND __source='" + opt.Source + "'"
		if _, err = dc.Exec(nil, q); err != nil {
			return err
		}
		if err = dc.VacuumAnalyzeTable(&t); err != nil {
			return err
		}
		q = "UPDATE " + dc.HistoryTableSQL(&t) + " SET __cf=TRUE,__end='" + now + "',__current=FALSE " +
			"WHERE NOT __cf AND __current AND __source='" + opt.Source + "'"
		if _, err = dc.Exec(nil, q); err != nil {
			return err
		}
		// Any non-current historical data can be set to __cf=TRUE.
		q = "UPDATE " + dc.HistoryTableSQL(&t) + " SET __cf=TRUE WHERE NOT __cf AND __source='" + opt.Source +
			"'"
		if _, err = dc.Exec(nil, q); err != nil {
			return err
		}
		if err = dc.VacuumAnalyzeTable(dc.HistoryTable(&t)); err != nil {
			return err
		}
	}
	eout.Info("completed clean")
	return nil
}

func sourceExists(dc sqlx.DB, sourceName string) (bool, error) {
	q := "SELECT 1 FROM metadb.source WHERE name='" + sourceName + "'"
	var i int64
	err := dc.QueryRow(nil, q).Scan(&i)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}
