package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

func Open(dbtype, dataSourceName string) (*DB, error) {
	switch dbtype {
	case "postgresql":
		return OpenPostgres(dataSourceName)
	case "redshift":
		return OpenRedshift(dataSourceName)
	default:
		return nil, fmt.Errorf("unknown database type: %s", dbtype)
	}
}

func PostgresDSN(host, port, dbname, user, password, sslmode string) string {
	return "host=" + host + " port=" + port + " user=" + user + " password=" + password + " dbname=" + dbname + " sslmode=" + sslmode
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
	Type DBType
}

type Table struct {
	Schema string
	Table  string
}

func NewTable(schema, table string) *Table {
	return &Table{Schema: schema,
		Table: table,
	}
}

type Column struct {
	Schema string
	Table  string
	Column string
}

func NewColumn(schema, table, column string) *Column {
	return &Column{Schema: schema,
		Table:  table,
		Column: column,
	}
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

func CSVToSQL(csv string) string {
	list := strings.Split(csv, ",")
	var b strings.Builder
	for i, s := range list {
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString("'" + strings.TrimSpace(s) + "'")
	}
	return b.String()
}
