package metadata

/*
import (
	"database/sql"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

func TrackRead(db sqlx.DB) (map[sqlx.Table]bool, error) {
	tables := make(map[sqlx.Table]bool)
	rows, err := db.Query(nil, "SELECT schemaname, tablename FROM metadb.track")
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

func TrackWrite(db sqlx.DB, table *sqlx.Table, transformed bool, parentTable *sqlx.Table) error {
	q := fmt.Sprintf("INSERT INTO metadb.track(schemaname,tablename,transformed,parentschema,parenttable)VALUES('%s','%s',%t,'%s','%s')", table.Schema, table.Table, transformed, parentTable.Schema, parentTable.Table)
	_, err := db.Exec(nil, q)
	if err != nil {
		return err
	}
	return nil
}
*/
