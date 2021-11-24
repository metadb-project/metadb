package upgrade

import (
	"fmt"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Upgrade(opt *option.Upgrade) error {
	// Require that a data directory be specified.
	if opt.Datadir == "" {
		return fmt.Errorf("data directory not specified")
	}
	var upgraded bool
	up, err := upgradeSysdb()
	if err != nil {
		return err
	}
	if up {
		upgraded = true
	}
	up, err = upgradeDatabases()
	if err != nil {
		return err
	}
	if up {
		upgraded = true
	}
	if !upgraded {
		eout.Info("databases are up to date")
	}
	return nil
}

func upgradeSysdb() (bool, error) {
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

	return true, nil
}

func upgradeDatabases() (bool, error) {
	dbcs, err := sysdb.ReadDatabaseConnectors()
	if err != nil {
		return false, err
	}
	var upgraded bool
	for _, dbc := range dbcs {
		up, err := upgradeDatabase(dbc)
		if err != nil {
			return false, err
		}
		if up {
			upgraded = true
		}
	}
	return upgraded, nil
}

func upgradeDatabase(dbc *sysdb.DatabaseConnector) (bool, error) {
	dbcname := "db." + dbc.Name
	db, err := sqlx.Open(dbc.Type,
		sqlx.PostgresDSN(dbc.DBHost, dbc.DBPort, dbc.DBName, dbc.DBAdminUser, dbc.DBAdminPassword, dbc.DBSSLMode))
	defer func(db *sqlx.DB) {
		_ = db.Close()
	}(db)
	if err != nil {
		return false, fmt.Errorf("%s: %s", dbcname, err)
	}
	if err = db.Ping(); err != nil {
		return false, fmt.Errorf("%s: %s", dbcname, err)
	}
	dbversion, err := metadata.GetDatabaseVersion(db)
	if err != nil {
		return false, fmt.Errorf("%s: %s", dbcname, err)
	}
	if dbversion == util.DatabaseVersion {
		return false, nil
	}
	if dbversion > util.DatabaseVersion {
		return false, fmt.Errorf("%s: database version incompatible with server (%d > %d)", dbcname, dbversion, util.DatabaseVersion)
	}

	return true, nil
}
