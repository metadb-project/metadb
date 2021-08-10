package sqlx

import (
	"database/sql"
	"fmt"
)

type DB struct {
	*sql.DB
}

type Table struct {
	Schema string
	Table  string
}

type Column struct {
	Schema string
	Table  string
	Column string
}

func (t *Table) String() string {
	return t.Schema + "." + t.Table
}

func (t *Table) SQL() string {
	return fmt.Sprintf("%q.%q", t.Schema, t.Table)
}

func (t *Table) History() *Table {
	return &Table{Schema: t.Schema, Table: t.HistoryTable()}
}

func (t *Table) HistoryTable() string {
	return t.Table + "__"
}
