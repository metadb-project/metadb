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
	fmt.Fprintf(os.Stderr, "metadb: remove all reset data in %q? ", opt.Connector)
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
	dsn, err := sysdb.ReadDataSourceName(opt.Connector)
	if err != nil {
		return err
	}
	db, err := sqlx.Open(dsn)
	if err != nil {
		return err
	}
	// Get list of tables
	tmap, err := metadata.TrackRead(db)
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
		_, err := db.ExecContext(context.TODO(), "DELETE FROM "+t.SQL()+" WHERE NOT __cf")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(context.TODO(), "UPDATE "+t.History().SQL()+" SET __cf=TRUE,__end='"+now+"',__current=FALSE WHERE NOT __cf AND __current")
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
