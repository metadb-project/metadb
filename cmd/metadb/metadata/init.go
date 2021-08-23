package metadata

import (
	"context"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

func Init(db *sqlx.DB) error {
	q := "CREATE SCHEMA IF NOT EXISTS metadb;"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating schema metadb: %s", err)
	}
	q = "" +
		"CREATE TABLE IF NOT EXISTS metadb.version (\n" +
		"    version BIGINT NOT NULL\n" +
		");"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table metadb.track: %s", err)
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
