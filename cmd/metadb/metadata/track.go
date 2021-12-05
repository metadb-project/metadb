package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

func TrackRead(db sqlx.DB) (map[sqlx.Table]bool, error) {
	tables := make(map[sqlx.Table]bool)
	q := "SELECT schemaname, tablename FROM metadb.track"
	rows, err := db.QueryContext(context.TODO(), q)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, err
		}
		tables[sqlx.Table{Schema: schema, Table: table}] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func TrackWrite(db sqlx.DB, table *sqlx.Table) error {
	q := "INSERT INTO metadb.track(schemaname,tablename,parentschema,parenttable)VALUES('" + table.Schema + "','" + table.Table + "','','')"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("insert: %v: %s", table, err)
	}
	return nil
}
