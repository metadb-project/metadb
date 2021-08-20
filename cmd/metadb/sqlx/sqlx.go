package sqlx

import (
	"context"
	"database/sql"
	"fmt"
)

func Open(dsn *DataSourceName) (*DB, error) {
	db, err := sql.Open("postgres", "host="+dsn.Host+" port="+dsn.Port+" user="+dsn.User+" password="+dsn.Password+" dbname="+dsn.DBName+" sslmode="+dsn.SSLMode)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

type DataSourceName struct {
	Host     string
	Port     string
	DBName   string
	User     string
	Password string
	SSLMode  string
}

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

func VacuumAnalyze(db *DB, table *Table) error {
	_, err := db.ExecContext(context.TODO(), "VACUUM "+table.SQL())
	if err != nil {
		return err
	}
	_, err = db.ExecContext(context.TODO(), "ANALYZE "+table.SQL())
	if err != nil {
		return err
	}
	_, err = db.ExecContext(context.TODO(), "VACUUM "+table.History().SQL())
	if err != nil {
		return err
	}
	_, err = db.ExecContext(context.TODO(), "ANALYZE "+table.History().SQL())
	if err != nil {
		return err
	}
	return nil
}
