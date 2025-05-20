package sysdb

import "github.com/metadb-project/metadb/cmd/metadb/types"

type TableSchema struct {
	Column []ColumnSchema
}

type ColumnSchema struct {
	Name       string
	DType      types.DataType
	DTypeSize  int64
	PrimaryKey int
	Data       any
}
