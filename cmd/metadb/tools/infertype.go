package tools

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/nazgaret/metadb/cmd/metadb/dbx"
)

func RefreshInferredColumnTypes(dq dbx.Queryable, progress func(string)) error {
	// Find all text columns.
	var q = `SELECT ns.nspname, t.relname, a.attname
    FROM metadb.base_table AS m
        JOIN pg_class AS t ON m.table_name||'__' = t.relname
        JOIN pg_namespace AS ns ON m.schema_name = ns.nspname AND t.relnamespace = ns.oid
        JOIN pg_attribute AS a ON t.oid = a.attrelid
        JOIN pg_type AS y ON a.atttypid = y.oid
    WHERE t.relkind IN ('r', 'p') AND a.attnum > 0 AND y.typname = 'text' AND left(attname, 2) <> '__'
    ORDER BY ns.nspname, t.relname, a.attnum`
	var rows pgx.Rows
	var err error
	if rows, err = dq.Query(context.TODO(), q); err != nil {
		return fmt.Errorf("selecting text columns: %v", err)
	}
	defer rows.Close()
	var columns = make([]dbx.Column, 0)
	for rows.Next() {
		var schema, table, column string
		if err = rows.Scan(&schema, &table, &column); err != nil {
			return fmt.Errorf("reading text columns: %v", err)
		}
		columns = append(columns, dbx.Column{Schema: schema, Table: table, Column: column})
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("reading text columns: %v", err)
	}
	rows.Close()

	for i := range columns {
		var col = columns[i]
		var colSpec = fmt.Sprintf("%s.%s (%s)", col.Schema, col.Table, col.Column)
		// Check if the column has any non-NULL values.
		var q = fmt.Sprintf("SELECT 1 FROM %s WHERE %s IS NOT NULL LIMIT 1",
			col.SchemaTableSQL(), col.ColumnSQL())
		var i int64
		err = dq.QueryRow(context.TODO(), q).Scan(&i)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			continue
		case err != nil:
			return fmt.Errorf("selecting NULL values: %s: %v", colSpec, err)
		default:
			// NOP: there is a non-NULL value.
		}

		if err = castColumn(dq, col, "uuid", progress); err != nil {
			return err
		}
		// Note that this method cannot be used to infer timestamp and timestamptz,
		// because PostgreSQL will freely cast to either.  A different method would be
		// needed, or a check would have to be added to test for the existence of a
		// timezone, in order to differentiate between the two types.
	}
	return nil
}

func castColumn(dq dbx.Queryable, col dbx.Column, ctype string, progress func(string)) error {
	var colSpec = fmt.Sprintf("%s.%s (%s)", col.Schema, col.Table, col.Column)
	// Check if all values can be cast to specific type.
	var q = fmt.Sprintf("SELECT count(*) FROM %s WHERE %s::%s = %s::%s",
		col.SchemaTableSQL(), col.ColumnSQL(), ctype, col.ColumnSQL(), ctype)
	var i int64
	var err = dq.QueryRow(context.TODO(), q).Scan(&i)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("testing cast to %s: %s: %v", ctype, colSpec, err)
	case err != nil:
		return nil // In this case we will take an error to mean the cast failed.
	default:
		// NOP: proceed with the cast
	}
	// Apply the cast.
	q = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s",
		col.SchemaTableSQL(), col.ColumnSQL(), ctype, col.ColumnSQL(), ctype)
	if _, err = dq.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("casting to %s: %s: %v", ctype, colSpec, err)
	}
	progress(fmt.Sprintf("converted %s to type %s", colSpec, ctype))
	q = fmt.Sprintf("CREATE INDEX ON %s (%s)", col.SchemaTableSQL(), col.ColumnSQL())
	if _, err = dq.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating index: %s: %v", colSpec, err)
	}
	return nil
}
