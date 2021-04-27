package server

import (
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
)

type deltaColumnSchema struct {
	newColumn   bool
	name        string
	oldType     command.DataType
	newType     command.DataType
	oldTypeSize int64
	newTypeSize int64
}

type deltaSchema struct {
	column []deltaColumnSchema
}

/*
func selectTableSchema(schema string, table string, svr *server) (*tableSchema, error) {
	var err error
	var q = fmt.Sprintf(""+
		"SELECT attr_name,\n"+
		"       attr_type,\n"+
		"       attr_type_size,\n"+
		"       pkey\n"+
		"    FROM dbsystem.attribute\n"+
		"    WHERE rel_schema = '%s' and rel_name = '%s';",
		schema, table)
	var rows *sql.Rows
	if rows, err = svr.db.QueryContext(context.TODO(), q); err != nil {
		return nil, fmt.Errorf("%s:\n%s", err, q)
	}
	defer rows.Close()
	var ts = new(tableSchema)
	for rows.Next() {
		var cs columnSchema
		var dtype string
		if err = rows.Scan(&cs.name, &dtype, &cs.dtypeSize, &cs.primaryKey); err != nil {
			return nil, err
		}
		cs.dtype = command.MakeDataType(dtype)

		ts.column = append(ts.column, cs)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return ts, nil
}
*/

func tableSchemaFromCommand(c *command.Command) *sysdb.TableSchema {
	var ts = new(sysdb.TableSchema)
	var col command.CommandColumn
	for _, col = range c.Column {
		var cs sysdb.ColumnSchema
		cs.Name = col.Name
		cs.DType = col.DType
		cs.DTypeSize = col.DTypeSize
		cs.PrimaryKey = col.PrimaryKey
		ts.Column = append(ts.Column, cs)
	}
	return ts
}

func getColumnSchema(tschema *sysdb.TableSchema, columnName string) *sysdb.ColumnSchema {
	var col sysdb.ColumnSchema
	for _, col = range tschema.Column {
		if col.Name == columnName {
			return &col
		}
	}
	return nil
}

func findDeltaColumnSchema(tschema string, tableName string, column1 *sysdb.ColumnSchema, column2 *sysdb.ColumnSchema, delta *deltaSchema) error {
	// If column does not exist, create a new one
	if column1 == nil {
		delta.column = append(delta.column, deltaColumnSchema{
			newColumn:   true,
			name:        column2.Name,
			newType:     column2.DType,
			newTypeSize: column2.DTypeSize,
		})
		return nil
	}
	// If the types are the same and the existing type size is larger than
	// the new one, the columns schema are compatible
	if column1.DType == column2.DType && column1.DTypeSize >= column2.DTypeSize {
		return nil
	}
	// Otherwise a type or size change is required.
	delta.column = append(delta.column, deltaColumnSchema{
		name:        column2.Name,
		oldType:     column1.DType,
		newType:     column2.DType,
		oldTypeSize: column1.DTypeSize,
		newTypeSize: column2.DTypeSize,
	})
	return nil

	///////////////////////////////////////////////////////////////////////////////

	// If the types are the same and they are varchar, both PostgreSQL and
	// Redshift can alter the column in place
	// if column1.Type == column2.Type && column1.Type == VarcharType {
	// 	if err = dbAlterColumnType(tschema, tableName,
	// 	column1.Name, column2.Type, column2.TypeSize, svr); err != nil {
	// 		return err
	// 	}
	// }

	// Increase data type size

	// TODO Rename column and create new column as above

	// fmt.Println("Type change not yet implemented:")
	// os.Exit(-1)

	// return nil
}

func findDeltaSchema(c *command.Command) (*deltaSchema, error) {
	var err error
	var schema1 *sysdb.TableSchema
	if schema1, err = sysdb.SelectTableSchema(c.SchemaName, c.TableName); err != nil {
		return nil, err
	}
	var schema2 *sysdb.TableSchema = tableSchemaFromCommand(c)
	var delta = new(deltaSchema)
	var col2 sysdb.ColumnSchema
	for _, col2 = range schema2.Column {
		var col1 *sysdb.ColumnSchema = getColumnSchema(schema1, col2.Name)
		if err = findDeltaColumnSchema(c.SchemaName, c.TableName, col1, &col2, delta); err != nil {
			return nil, err
		}
	}
	// findDeltaPrimaryKey()
	return delta, nil
}
