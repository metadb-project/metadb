package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

type ColumnType struct {
	DataType   string
	CharMaxLen int64
}

func (c *Catalog) initSchema() error {
	columns := make(map[sqlx.Column]ColumnType)
	// Read column schemas from database.
	columnSchemas, err := getColumnSchemas(c.dp)
	if err != nil {
		return fmt.Errorf("reading column schemas: %s", err)
	}
	for _, col := range columnSchemas {
		if !c.TableExists(dbx.Table{S: col.Schema, T: col.Table}) {
			continue
		}
		//if !track.Contains(&sqlx.T{S: col.S, T: col.T}) {
		//	continue
		//}
		c := sqlx.Column{Schema: col.Schema, Table: col.Table, Column: col.Column}
		columns[c] = ColumnType{DataType: col.DataType, CharMaxLen: *col.CharMaxLen}
	}
	c.columns = columns
	return nil
}

func getColumnSchemas(dp *pgxpool.Pool) ([]*sqlx.ColumnSchema, error) {
	cs := make([]*sqlx.ColumnSchema, 0)
	rows, err := dp.Query(context.TODO(), ""+
		"SELECT table_schema, left(table_name, -2) table_name, column_name, data_type, character_maximum_length "+
		"FROM information_schema.columns "+
		"WHERE lower(table_schema) NOT IN ('information_schema', 'pg_catalog')"+
		" AND right(table_name, 2) = '__'"+
		" AND lower(column_name) NOT IN ('__id', '__cf', '__start', '__end', '__current', '__source', '__origin');")
	if err != nil {
		return nil, fmt.Errorf("querying database schema: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		var schema, table, column, dataType string
		var charMaxLenNull *int64
		if err := rows.Scan(&schema, &table, &column, &dataType, &charMaxLenNull); err != nil {
			return nil, fmt.Errorf("reading data from database schema: %s", err)
		}
		var charMaxLen int64
		if charMaxLenNull != nil {
			charMaxLen = *charMaxLenNull
		}
		schemaName := schema
		tableName := table
		columnName := column
		c := &sqlx.ColumnSchema{
			Schema:     schemaName,
			Table:      tableName,
			Column:     columnName,
			DataType:   dataType,
			CharMaxLen: &charMaxLen,
		}
		cs = append(cs, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading schema from database catalog: %v", err)
	}
	return cs, nil
}

func (c *Catalog) UpdateColumn(column *sqlx.Column, dataType string, charMaxLen int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	updateColumn(c, column, dataType, charMaxLen)
}

func updateColumn(cat *Catalog, column *sqlx.Column, dataType string, charMaxLen int64) {
	cat.columns[*column] = ColumnType{DataType: dataType, CharMaxLen: charMaxLen}
}

func (c *Catalog) DeleteColumn(column *sqlx.Column) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.columns, *column)
}

func (c *Catalog) TableColumns(table *sqlx.Table) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var columns []string
	for k := range c.columns {
		if k.Schema == table.Schema && k.Table == table.Table {
			columns = append(columns, k.Column)
		}
	}
	return columns
}

func (c *Catalog) TableSchema(table *sqlx.Table) map[string]ColumnType {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts := make(map[string]ColumnType)
	for k, v := range c.columns {
		if k.Schema == table.Schema && k.Table == table.Table {
			ts[k.Column] = v
		}
	}
	return ts
}

func (c *Catalog) Column(column *sqlx.Column) *ColumnType {
	c.mu.Lock()
	defer c.mu.Unlock()
	cs, ok := c.columns[*column]
	if ok {
		return &cs
	}
	return nil
}

// TODO Use txn for ALTER TABLE and CREATE INDEX to ensure database and cache are updated atomically.
func (c *Catalog) AddColumn(table dbx.Table, columnName string, newType command.DataType, newTypeSize int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Alter table schema in database.
	dataTypeSQL, dataType, charMaxLen := command.DataTypeToSQL(newType, newTypeSize)
	_, err := c.dp.Exec(context.TODO(), "ALTER TABLE "+table.MainSQL()+" ADD COLUMN \""+columnName+"\" "+dataTypeSQL)
	if err != nil {
		return fmt.Errorf("adding column %q in table %q: alter table: %v", columnName, table, err)
	}
	// Update schema.
	updateColumn(c, &sqlx.Column{Schema: table.S, Table: table.T, Column: columnName}, dataType, charMaxLen)
	return nil
}
