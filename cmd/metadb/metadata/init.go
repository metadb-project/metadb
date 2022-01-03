package metadata

import (
	"database/sql"
	"fmt"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Init(db sqlx.DB, metadbVersion string) error {
	// Check if initialized
	initialized, err := isDatabaseInitialized(db)
	if err != nil {
		return err
	}
	if initialized {
		// Database already initialized
		// Check that database version is compatible
		err = ValidateDatabaseVersion(db)
		if err != nil {
			return err
		}
		// Set Metadb version
		_, err = db.Exec(nil, "UPDATE metadb.init SET version = '"+metadbVersionString(metadbVersion)+"'")
		if err != nil {
			return fmt.Errorf("updating table metadb.init: %s", err)
		}
		return nil
	}
	// Initialize
	_, err = db.Exec(nil, "CREATE SCHEMA IF NOT EXISTS metadb")
	if err != nil {
		return fmt.Errorf("creating schema metadb: %s", err)
	}
	_, err = db.Exec(nil, ""+
		"CREATE TABLE IF NOT EXISTS metadb.init (\n"+
		"    version VARCHAR(80) NOT NULL,\n"+
		"    dbversion INTEGER NOT NULL\n"+
		")")
	if err != nil {
		return fmt.Errorf("creating table metadb.track: %s", err)
	}
	mdbVersion := metadbVersionString(metadbVersion)
	dbVersion := fmt.Sprintf("%d", util.DatabaseVersion)
	_, err = db.Exec(nil, "INSERT INTO metadb.init (version, dbversion) VALUES ('"+mdbVersion+"', "+dbVersion+")")
	if err != nil {
		return fmt.Errorf("writing to table metadb.init: %s", err)
	}
	_, err = db.Exec(nil, ""+
		"CREATE TABLE IF NOT EXISTS metadb.track (\n"+
		"    schemaname VARCHAR(63) NOT NULL,\n"+
		"    tablename VARCHAR(63) NOT NULL,\n"+
		"    PRIMARY KEY (schemaname, tablename),\n"+
		"    parentschema VARCHAR(63) NOT NULL,\n"+
		"    parenttable VARCHAR(63) NOT NULL\n"+
		")")
	if err != nil {
		return fmt.Errorf("creating table metadb.track: %s", err)
	}
	return nil
}

func isDatabaseInitialized(db sqlx.DB) (bool, error) {
	var v int64
	err := db.QueryRow(nil, "SELECT dbversion FROM metadb.init LIMIT 1").Scan(&v)
	switch {
	case err == sql.ErrNoRows:
		return false, fmt.Errorf("reading from table metadb.init: %s", err)
	case err != nil:
		return false, nil
	default:
		return true, nil
	}
}

func metadbVersionString(metadbVersion string) string {
	return "Metadb " + metadbVersion
}

func ValidateDatabaseVersion(db sqlx.DB) error {
	dbversion, err := GetDatabaseVersion(db)
	if err != nil {
		return err
	}
	if dbversion != util.DatabaseVersion {
		m := fmt.Sprintf("database incompatible with server (%d != %d)", dbversion, util.DatabaseVersion)
		if dbversion < util.DatabaseVersion {
			m = m + ": upgrade using \"metadb upgrade\""
		}
		return fmt.Errorf("%s", m)
	}
	return nil
}

func GetDatabaseVersion(db sqlx.DB) (int64, error) {
	var dbversion int64
	err := db.QueryRow(nil, "SELECT dbversion FROM metadb.init").Scan(&dbversion)
	switch {
	case err == sql.ErrNoRows:
		return 0, fmt.Errorf("unable to query dbversion")
	case err != nil:
		return 0, fmt.Errorf("querying dbversion: %s", err)
	default:
		return dbversion, nil
	}
}
