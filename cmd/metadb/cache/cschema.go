package cache

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

type Schema struct {
	columns map[sqlx.Column]ColumnSchema
	track   *Track
}

type ColumnSchema struct {
	DataType   string
	CharMaxLen int64
}

func NewSchema(db sqlx.DB, track *Track) (*Schema, error) {
	columns := make(map[sqlx.Column]ColumnSchema)
	// read schema from database
	q := "" +
		"SELECT table_schema, table_name, column_name, data_type, character_maximum_length\n" +
		"    FROM information_schema.columns\n" +
		"    WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND\n" +
		"          table_name NOT LIKE '%\\_\\_' AND\n" +
		"          column_name NOT IN ('__id', '__start', '__origin');"
	rows, err := db.QueryContext(context.TODO(), q)
	if err != nil {
		return nil, fmt.Errorf("querying database schema: %s", err)
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	for rows.Next() {
		var schemaName, tableName, columnName, dataType string
		var charMaxLenNull sql.NullInt64
		if err := rows.Scan(&schemaName, &tableName, &columnName, &dataType, &charMaxLenNull); err != nil {
			return nil, fmt.Errorf("reading data from database schema: %s", err)
		}
		var charMaxLen int64
		if charMaxLenNull.Valid {
			charMaxLen = charMaxLenNull.Int64
		}
		if !track.Contains(&sqlx.Table{Schema: schemaName, Table: tableName}) {
			continue
		}
		c := sqlx.Column{Schema: schemaName, Table: tableName, Column: columnName}
		columns[c] = ColumnSchema{DataType: dataType, CharMaxLen: charMaxLen}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading data from database schema: %s", err)
	}
	return &Schema{columns: columns, track: track}, nil
}

func (s *Schema) Update(column *sqlx.Column, dataType string, charMaxLen int64) {
	s.columns[*column] = ColumnSchema{DataType: dataType, CharMaxLen: charMaxLen}
}

func (s *Schema) Delete(column *sqlx.Column) {
	delete(s.columns, *column)
}

func (s *Schema) TableColumns(table *sqlx.Table) []string {
	var columns []string
	for k := range s.columns {
		if k.Schema == table.Schema && k.Table == table.Table {
			columns = append(columns, k.Column)
		}
	}
	return columns
}

func (s *Schema) TableSchema(table *sqlx.Table) map[string]ColumnSchema {
	ts := make(map[string]ColumnSchema)
	for k, v := range s.columns {
		if k.Schema == table.Schema && k.Table == table.Table {
			ts[k.Column] = v
		}
	}
	return ts
}

func (s *Schema) Column(column *sqlx.Column) *ColumnSchema {
	cs, ok := s.columns[*column]
	if ok {
		return &cs
	}
	return nil
}
