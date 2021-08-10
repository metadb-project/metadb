package sysdb

import (
	"github.com/metadb-project/metadb/cmd/metadb/command"
)

type TableSchema struct {
	Column []ColumnSchema
}

type ColumnSchema struct {
	Name       string
	DType      command.DataType
	DTypeSize  int64
	PrimaryKey int
}
