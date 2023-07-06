package server

import (
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
)

func tableSchemaFromCommand(c *command.Command) *sysdb.TableSchema {
	var ts = new(sysdb.TableSchema)
	var col command.CommandColumn
	for _, col = range c.Column {
		var cs sysdb.ColumnSchema
		cs.Name = col.Name
		cs.DType = col.DType
		cs.DTypeSize = col.DTypeSize
		cs.PrimaryKey = col.PrimaryKey
		cs.Data = col.Data
		ts.Column = append(ts.Column, cs)
	}
	return ts
}

func getColumnSchema(tschema *sysdb.TableSchema, columnName string) *sysdb.ColumnSchema {
	for i := range tschema.Column {
		if tschema.Column[i].Name == columnName {
			return &(tschema.Column[i])
		}
	}
	return nil
}

func findDeltaColumnSchema(column1 *sysdb.ColumnSchema, column2 *sysdb.ColumnSchema, delta *deltaSchema) {
	// If column does not exist, create a new one
	if column1 == nil {
		delta.column = append(delta.column, deltaColumnSchema{
			newColumn:   true,
			name:        column2.Name,
			newType:     column2.DType,
			newTypeSize: column2.DTypeSize,
			newData:     column2.Data,
		})
		return
	}
	// If the types are the same and the existing type size is greater than or equal
	// to the new one, the column schemas are compatible.
	if column1.DType == column2.DType && column1.DTypeSize >= column2.DTypeSize {
		return
	}
	// Otherwise a type or size change is required.
	delta.column = append(delta.column, deltaColumnSchema{
		name:        column2.Name,
		oldType:     column1.DType,
		newType:     column2.DType,
		oldTypeSize: column1.DTypeSize,
		newTypeSize: column2.DTypeSize,
		newData:     column2.Data,
	})
	return
}

type deltaColumnSchema struct {
	newColumn   bool
	name        string
	oldType     command.DataType
	newType     command.DataType
	oldTypeSize int64
	newTypeSize int64
	newData     any
}

type deltaSchema struct {
	column []deltaColumnSchema
}
