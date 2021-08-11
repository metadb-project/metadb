package sqlx

import (
	"context"
	"database/sql"
	"fmt"
)

// MakeTx creates a new transaction.
func MakeTx(db *sql.DB) (*sql.Tx, error) {
	tx, err := db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	return tx, nil
}

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
