package sqlx

import (
	"database/sql"
	"fmt"
	"strings"
)

type DB interface {
	Close()
	Ping() error
	VacuumAnalyzeTable(table *Table) error
	EncodeString(s string) string
	ExecMultiple(tx *sql.Tx, sql []string) error
	Exec(tx *sql.Tx, sql string) (sql.Result, error)
	Query(tx *sql.Tx, query string) (*sql.Rows, error)
	QueryRow(tx *sql.Tx, query string) *sql.Row
	HistoryTableSQL(table *Table) string
	HistoryTable(table *Table) *Table
	TableSQL(table *Table) string
	IdentiferSQL(id string) string
	AutoIncrementSQL() string
	BeginTx() (*sql.Tx, error)
}

type ColumnSchema struct {
	Schema     string
	Table      string
	Column     string
	DataType   string
	CharMaxLen *int64
}

func Open(dbtype string, dataSourceName *DSN) (DB, error) {
	switch dbtype {
	case "postgresql":
		return OpenPostgres(dataSourceName)
	default:
		return nil, fmt.Errorf("unknown database type: %s", dbtype)
	}
}

type DSN struct {
	// DBURI string
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
	// Account  string
}

type Table struct {
	Schema string
	Table  string
}

func NewTable(schema, table string) *Table {
	return &Table{
		Schema: schema,
		Table:  table,
	}
}

type Column struct {
	Schema string
	Table  string
	Column string
}

func NewColumn(schema, table, column string) *Column {
	return &Column{
		Schema: schema,
		Table:  table,
		Column: column,
	}
}

func (t *Table) String() string {
	return t.Schema + "." + t.Table
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
