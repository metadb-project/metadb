package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/types"
)

/*
type ColumnType struct {
	DataType   string
	CharMaxLen int64
}
*/

func (c *Catalog) initSchema() error {
	columns := make(map[dbx.Column]string)
	// Read column schemas from database.
	columnSchemas, err := getColumnSchemas(c.dp)
	if err != nil {
		return fmt.Errorf("reading column schemas: %s", err)
	}
	for _, col := range columnSchemas {
		if !c.TableExists(&dbx.Table{Schema: col.Schema, Table: col.Table}) {
			continue
		}
		//if !track.Contains(&sqlx.Table{Schema: col.Schema, Table: col.Table}) {
		//	continue
		//}
		c := dbx.Column{Schema: col.Schema, Table: col.Table, Column: col.Column}
		columns[c] = col.DataType
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
		" AND lower(column_name) NOT IN ('__id', '__start', '__end', '__current', '__origin');")
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
		return nil, fmt.Errorf("reading schema from database catalog: %w", err)
	}
	return cs, nil
}

func (c *Catalog) UpdateColumn(column *dbx.Column, dataType string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	updateColumn(c, column, dataType)
}

func updateColumn(cat *Catalog, column *dbx.Column, dataType string) {
	cat.columns[*column] = dataType
}

func (c *Catalog) DeleteColumn(column *dbx.Column) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.columns, *column)
}

func (c *Catalog) TableColumns(table *dbx.Table) []string {
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

func (c *Catalog) TableSchema(table *dbx.Table) map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts := make(map[string]string)
	for k, v := range c.columns {
		if k.Schema == table.Schema && k.Table == table.Table {
			ts[k.Column] = v
		}
	}
	return ts
}

func (c *Catalog) Column(column *dbx.Column) *string {
	c.mu.Lock()
	defer c.mu.Unlock()
	cs, ok := c.columns[*column]
	if ok {
		return &cs
	}
	return nil
}

func (c *Catalog) AddColumn(table *dbx.Table, columnName string, newType types.DataType, newTypeSize int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Alter table schema in database.
	dataTypeSQL := types.DataTypeToSQL(newType, newTypeSize)
	q := "ALTER TABLE " + table.MainSQL() + " ADD COLUMN \"" + columnName + "\" " + dataTypeSQL
	if c.lz4 && (newType == types.TextType || newType == types.JSONType) {
		q = q + " COMPRESSION lz4"
	}
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("adding column %q in table %q: alter table: %v", columnName, table, err)
	}
	// Create index if type is uuid.
	if newType == types.UUIDType {
		column := &dbx.Column{Schema: table.Schema, Table: table.Table, Column: columnName}
		if !c.indexExists(column) {
			if err := c.addIndex(column); err != nil {
				return err
			}
		}
	}
	// Update schema.
	updateColumn(c, &dbx.Column{Schema: table.Schema, Table: table.Table, Column: columnName}, dataTypeSQL)
	return nil
}
