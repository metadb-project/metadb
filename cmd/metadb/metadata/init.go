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
		"    version VARCHAR(255) NOT NULL,\n" +
		"    dbversion BIGINT NOT NULL\n" +
		");"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table metadb.track: %s", err)
	}
	// The database version is hardcoded at the moment, but it needs to be
	// synchronized with sysdb.
	q = "INSERT INTO metadb.init (version, dbversion) VALUES ('" + metadbVersionString(metadbVersion) + "', 4)"
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
