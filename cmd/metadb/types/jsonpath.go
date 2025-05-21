package types

import (
	"fmt"
	"strings"
)

type JSONPath struct {
	Schema string
	Table  string
	Column string
	Path   [16]string
}

func NewJSONPath(schema, table, column string, path string) JSONPath {
	k := JSONPath{
		Schema: schema,
		Table:  table,
		Column: column,
	}
	if path != "" {
		s := strings.Split(path, ".")
		for i := 1; i < len(s); i++ {
			k.Path[i-1] = s[i]
		}
	}
	return k
}

func (j JSONPath) Append(node string) JSONPath {
	k := JSONPath{
		Schema: j.Schema,
		Table:  j.Table,
		Column: j.Column,
	}
	for i := 0; i < len(j.Path); i++ {
		if j.Path[i] == "" {
			k.Path[i] = node
			return k
		} else {
			k.Path[i] = j.Path[i]
		}
	}
	panic(fmt.Sprintf("JSON path exceeds limit of %d nodes", len(j.Path)))
}
