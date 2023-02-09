package cache

/*type Schema struct {
	columns map[sqlx.Column]ColumnType
	cat     *catalog.Catalog
}

type ColumnType struct {
	DataType   string
	CharMaxLen int64
}

func NewSchema(db sqlx.DB, cat *catalog.Catalog) (*Schema, error) {
	columns := make(map[sqlx.Column]ColumnType)
	// Read column schemas from database.
	columnSchemas, err := getColumnSchemas(db)
	if err != nil {
		return nil, fmt.Errorf("reading column schemas: %s", err)
	}
	for _, col := range columnSchemas {
		if !cat.TableExists(dbx.Table{S: col.Schema, T: col.Table}) {
			continue
		}
		//if !track.Contains(&sqlx.T{S: col.S, T: col.T}) {
		//	continue
		//}
		c := sqlx.Column{Schema: col.Schema, Table: col.Table, Column: col.Column}
		columns[c] = ColumnType{DataType: col.DataType, CharMaxLen: *col.CharMaxLen}
	}
	return &Schema{columns: columns, cat: cat}, nil
}

func getColumnSchemas(db sqlx.DB) ([]*sqlx.ColumnSchema, error) {
	cs := make([]*sqlx.ColumnSchema, 0)
	rows, err := db.Query(nil, ""+
		"SELECT table_schema, left(table_name, -2) table_name, column_name, data_type, character_maximum_length "+
		"FROM information_schema.columns "+
		"WHERE lower(table_schema) NOT IN ('information_schema', 'pg_catalog')"+
		" AND right(table_name, 2) = '__'"+
		" AND lower(column_name) NOT IN ('__id', '__cf', '__start', '__end', '__current', '__source', '__origin');")
	if err != nil {
		return nil, fmt.Errorf("querying database schema: %s", err)
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	for rows.Next() {
		var schema, table, column, dataType string
		var charMaxLenNull sql.NullInt64
		if err := rows.Scan(&schema, &table, &column, &dataType, &charMaxLenNull); err != nil {
			return nil, fmt.Errorf("reading data from database schema: %s", err)
		}
		var charMaxLen int64
		if charMaxLenNull.Valid {
			charMaxLen = charMaxLenNull.Int64
		}
		// For Snowflake, convert to lowercase, for example:
		//     schemaName := strings.ToLower(schema)
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
		return nil, fmt.Errorf("reading data from database schema: %s", err)
	}
	return cs, nil
}

func (s *Schema) Update(column *sqlx.Column, dataType string, charMaxLen int64) {
	s.columns[*column] = ColumnType{DataType: dataType, CharMaxLen: charMaxLen}
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

func (s *Schema) TableSchema(table *sqlx.Table) map[string]ColumnType {
	ts := make(map[string]ColumnType)
	for k, v := range s.columns {
		if k.Schema == table.Schema && k.Table == table.Table {
			ts[k.Column] = v
		}
	}
	return ts
}

func (s *Schema) Column(column *sqlx.Column) *ColumnType {
	cs, ok := s.columns[*column]
	if ok {
		return &cs
	}
	return nil
}
*/
