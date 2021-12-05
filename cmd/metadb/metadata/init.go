package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Init(db *sqlx.DB, metadbVersion string) error {
	// Check if initialized
	var dbver int64
	err := db.QueryRowContext(context.TODO(), "SELECT dbversion FROM metadb.init LIMIT 1").Scan(&dbver)
	switch {
	case err == sql.ErrNoRows:
		return fmt.Errorf("checking for database initialization: %s", err)
	case err != nil:
		// NOP: database not initialized
	default:
		// Database already initialized
		// Check that database version is compatible
		err = ValidateDatabaseVersion(db)
		if err != nil {
			return err
		}
		// Set Metadb version
		q := "UPDATE metadb.init SET version = '" + metadbVersionString(metadbVersion) + "'"
		if _, err := db.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("updating table metadb.init: %s", err)
		}
		return nil
	}
	// Initialize
	q := "CREATE SCHEMA IF NOT EXISTS metadb;"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating schema metadb: %s", err)
	}
	q = "" +
		"CREATE TABLE IF NOT EXISTS metadb.init (\n" +
		"    version VARCHAR(80) NOT NULL,\n" +
		"    dbversion INTEGER NOT NULL\n" +
		");"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table metadb.track: %s", err)
	}
	mdbVersion := metadbVersionString(metadbVersion)
	dbVersion := fmt.Sprintf("%d", util.DatabaseVersion)
	q = "INSERT INTO metadb.init (version, dbversion) VALUES ('" + mdbVersion + "', " + dbVersion + ")"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("writing to table metadb.init: %s", err)
	}
	q = "" +
		"CREATE TABLE IF NOT EXISTS metadb.track (\n" +
		"    schemaname VARCHAR(63) NOT NULL,\n" +
		"    tablename VARCHAR(63) NOT NULL,\n" +
		"    PRIMARY KEY (schemaname, tablename),\n" +
		"    parentschema VARCHAR(63) NOT NULL,\n" +
		"    parenttable VARCHAR(63) NOT NULL\n" +
		");"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table metadb.track: %s", err)
	}
	return nil
}

func metadbVersionString(metadbVersion string) string {
	return "Metadb " + metadbVersion
}

func ValidateDatabaseVersion(db *sqlx.DB) error {
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

func GetDatabaseVersion(db *sqlx.DB) (int64, error) {
	q := "SELECT dbversion FROM metadb.init"
	var dbversion int64
	err := db.QueryRowContext(context.TODO(), q).Scan(&dbversion)
	switch {
	case err == sql.ErrNoRows:
		return 0, fmt.Errorf("unable to query dbversion")
	case err != nil:
		return 0, fmt.Errorf("querying dbversion: %s", err)
	default:
		return dbversion, nil
	}
}
