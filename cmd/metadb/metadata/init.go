package metadata

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

func Init(db *sqlx.DB, metadbVersion string) error {
	// Check if initialized
	var dbver int64
	err := db.QueryRowContext(context.TODO(), "SELECT database_version FROM metadb.version LIMIT 1").Scan(&dbver)
	switch {
	case err == sql.ErrNoRows:
		return fmt.Errorf("checking for database initialization: %s", err)
	case err != nil:
		// NOP: database not initialized
	default:
		// Database already initialized
		q := "UPDATE metadb.version SET metadb_version = '" + metadbVersionString(metadbVersion) + "'"
		if _, err := db.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("updating table metadb.version: %s", err)
		}
		return nil
	}
	// Initialize
	q := "CREATE SCHEMA IF NOT EXISTS metadb;"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating schema metadb: %s", err)
	}
	q = "" +
		"CREATE TABLE IF NOT EXISTS metadb.version (\n" +
		"    metadb_version VARCHAR(255) NOT NULL,\n" +
		"    database_version BIGINT NOT NULL\n" +
		");"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table metadb.track: %s", err)
	}
	q = "INSERT INTO metadb.version (metadb_version, database_version) VALUES ('" + metadbVersionString(metadbVersion) + "', 1)"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("writing to table metadb.version: %s", err)
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
