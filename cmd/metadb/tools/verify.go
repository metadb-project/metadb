package tools

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
)

func VerifyConsistency(dq dbx.Queryable, progress func(string)) error {
	if err := verifyTransformTableSize(dq, progress); err != nil {
		return fmt.Errorf("verifying transform table size")
	}
	return nil
}

func verifyTransformTableSize(dq dbx.Queryable, progress func(string)) error {
	// Read all table names.
	q := "SELECT schema_name, table_name FROM metadb.base_table"
	tableSizes := make(map[dbx.Table]int64)
	var schema, table string
	rows, _ := dq.Query(context.TODO(), q)
	_, err := pgx.ForEachRow(rows, []any{&schema, &table}, func() error {
		tableSizes[dbx.Table{Schema: schema, Table: table}] = 0
		return nil
	})
	if err != nil {
		return fmt.Errorf("reading table names: %w", err)
	}
	// Read table row oounts.
	for t := range tableSizes {
		q = fmt.Sprintf("SELECT count(*) FROM %s", t.SQL())
		var i int64
		err = dq.QueryRow(context.TODO(), q).Scan(&i)
		if err != nil {
			return fmt.Errorf("checking row count for table %s: %v", t, err)
		}
		tableSizes[t] = i
	}
	// Read transform tables and parents.
	q = "SELECT schema_name, table_name, parent_schema_name, parent_table_name FROM metadb.base_table " +
		"WHERE transformed"
	var parentSchema, parentTable string
	rows, _ = dq.Query(context.TODO(), q)
	_, err = pgx.ForEachRow(rows, []any{&schema, &table, &parentSchema, &parentTable}, func() error {
		t := dbx.Table{Schema: schema, Table: table}
		p := dbx.Table{Schema: parentSchema, Table: parentTable}
		if tableSizes[t] != tableSizes[p] {
			progress(fmt.Sprintf("possible record count mismatch: %s != %s", p, t))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("reading table relationships: %w", err)
	}
	return nil
}
