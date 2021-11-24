package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/metadb-project/metadb/cmd/metadb/util"

	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

func Init(db sqlx.DB, metadbVersion string) error {
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
	q := "SELECT dbversion FROM metadb.init"
	var databaseVersion int64
	err := db.QueryRowContext(context.TODO(), q).Scan(&databaseVersion)
	switch {
	case err == sql.ErrNoRows:
		return fmt.Errorf("unable to query dbversion")
	case err != nil:
		return fmt.Errorf("querying dbversion: %s", err)
	default:
		if databaseVersion == util.DatabaseVersion {
			return nil
		} else {
			m := fmt.Sprintf("database incompatible with server (%d != %d)", databaseVersion, util.DatabaseVersion)
			if databaseVersion < util.DatabaseVersion {
				m = m + ": upgrade using \"metadb upgrade\""
			}
			return fmt.Errorf("%s", m)
		}
	}
}
