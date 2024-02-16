package sysdb

import (
	"github.com/nazgaret/metadb/cmd/metadb/command"
)

type TableSchema struct {
	Column []ColumnSchema
}

type ColumnSchema struct {
	Name       string
	DType      command.DataType
	DTypeSize  int64
	PrimaryKey int
	Data       any
}
