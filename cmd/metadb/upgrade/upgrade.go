package upgrade

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

type databaseState struct {
	tx   *sql.Tx
	db   sqlx.DB
	name string
	dbc  *sysdb.DatabaseConnector
}

func Upgrade(opt *option.Upgrade) error {
	// Require that a data directory be specified.
	if opt.Datadir == "" {
		return fmt.Errorf("data directory not specified")
	}
	// Open sysdb
	dsnsys := "file:" + util.SysdbFileName(opt.Datadir) + sysdb.OpenOptions
	dbsys, err := sql.Open("sqlite3", dsnsys)
	if err != nil {
		return err
	}
	defer func(dbsys *sql.DB) {
		_ = dbsys.Close()
	}(dbsys)
	txsys, err := dbsys.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer func(txsys *sql.Tx) {
		_ = txsys.Rollback()
	}(txsys)
	// Open databases
	dbcs, err := sysdb.ReadDatabaseConnectors()
	if err != nil {
		return err
	}
	dbstate := make([]databaseState, 0)
	for _, dbc := range dbcs {
		name := dbc.Name
		dsn := &sqlx.DSN{
			Host:     dbc.DBHost,
			Port:     dbc.DBPort,
			User:     dbc.DBAdminUser,
			Password: dbc.DBAdminPassword,
			DBName:   dbc.DBName,
			SSLMode:  dbc.DBSSLMode,
			Account:  dbc.DBAccount,
		}
		var db sqlx.DB
		db, err = sqlx.Open(dbc.Name, dbc.Type, dsn)
		if err != nil {
			return err
		}
		// TODO - Defer in loops can cause leaks.
		defer db.Close()
		if err = db.Ping(); err != nil {
			return err
		}
		var tx *sql.Tx
		tx, err = db.BeginTx()
		if err != nil {
			return err
		}
		// TODO - Defer in loops can cause leaks.
		defer func(tx *sql.Tx) {
			_ = tx.Rollback()
		}(tx)
		dbst := databaseState{
			tx:   tx,
			db:   db,
			name: name,
			dbc:  dbc,
		}
		dbstate = append(dbstate, dbst)
	}
	// Upgrade sysdb
	var upgraded bool
	up, err := upgradeSysdb(txsys)
	if err != nil {
		return err
	}
	if up {
		upgraded = true
	}
	// Upgrade databases
	up, err = upgradeDatabases(dbstate)
	if err != nil {
		return err
	}
	if up {
		upgraded = true
	}
	// Commit all
	if err = txsys.Commit(); err != nil {
		return err
	}
	for _, dbst := range dbstate {
		if err = dbst.tx.Commit(); err != nil {
			return err
		}
	}
	// Write message if nothing was upgraded
	if !upgraded {
		eout.Info("databases are up to date")
	}
	return nil
}

// TODO - Txn is unused
func upgradeSysdb(tx *sql.Tx) (bool, error) {
	dbversion, err := sysdb.GetSysdbVersion()
	if err != nil {
		return false, err
	}
	if dbversion == util.DatabaseVersion {
		return false, nil
	}
	if dbversion > util.DatabaseVersion {
		return false, fmt.Errorf("data directory version incompatible with server (%d > %d)", dbversion, util.DatabaseVersion)
	}
	if err = upgradeSysdbAll(dbversion); err != nil {
		return false, err
	}
	return true, nil
}

// type sysdbOpt struct {
// }

// type upgradeFunc func(p []byte) (n int, err error)

func upgradeSysdbAll(dbversion int64) error {
	for v := dbversion + 1; v <= util.DatabaseVersion; v++ {
		// database_upgrades[v](&opt);
	}

	return nil
}

func upgradeDatabases(dbstate []databaseState) (bool, error) {
	var upgraded bool
	for _, dbst := range dbstate {
		up, err := upgradeDatabase(dbst)
		if err != nil {
			return false, err
		}
		if up {
			upgraded = true
		}
	}
	return upgraded, nil
}

func upgradeDatabase(dbst databaseState) (bool, error) {
	dbversion, err := metadata.GetDatabaseVersion(dbst.db)
	if err != nil {
		return false, fmt.Errorf("%s: %s", dbst.name, err)
	}
	if dbversion == util.DatabaseVersion {
		return false, nil
	}
	if dbversion > util.DatabaseVersion {
		return false, fmt.Errorf("%s: database version incompatible with server (%d > %d)",
			dbst.name, dbversion, util.DatabaseVersion)
	}

	return true, nil
}
