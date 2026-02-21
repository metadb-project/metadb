package libpq

import (
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/types"
)

func alterTableAddColumn(conn net.Conn, node *ast.AlterTableAddColumnStmt, dc *pgx.Conn, cat *catalog.Catalog) error {
	if err := validateColumnType(node.ColumnType); err != nil {
		return err
	}

	table, err := parseMainTableName(node.TableName)
	if err != nil {
		return err
	}

	// ensure table is in the catalog
	if !cat.TableExists(&table) {
		return fmt.Errorf("data table %q does not exist", node.TableName)
	}

	// ensure column does not exist
	column := dbx.Column{Schema: table.Schema, Table: table.Table, Column: node.ColumnName}
	if cat.ColumnType(&column) != nil {
		return fmt.Errorf("column %q of table %q already exists", node.ColumnName, node.TableName)
	}

	_ = writeEncoded(conn, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "INFO",
		Message: "waiting for stream processor lock"},
	})

	catalog.ExecMutex.Lock()
	defer catalog.ExecMutex.Unlock()

	dataType, typeSize := types.MakeDataType(node.ColumnType)
	if err = cat.AddColumn(&table, node.ColumnName, dataType, typeSize); err != nil {
		return err
	}
	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func alterTableAlterColumn(conn net.Conn, node *ast.AlterTableAlterColumnStmt, dc *pgx.Conn, cat *catalog.Catalog) error {
	if err := validateColumnType(node.ColumnType); err != nil {
		return err
	}

	table, err := parseMainTableName(node.TableName)
	if err != nil {
		return err
	}

	// ensure table is in the catalog
	if !cat.TableExists(&table) {
		return fmt.Errorf("data table %q does not exist", node.TableName)
	}

	// ensure column exists
	column := dbx.Column{Schema: table.Schema, Table: table.Table, Column: node.ColumnName}
	t := cat.ColumnType(&column)
	if t == nil {
		return fmt.Errorf("column %q of table %q does not exist", node.ColumnName, node.TableName)
	}

	_ = writeEncoded(conn, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "INFO",
		Message: "waiting for stream processor lock"},
	})

	catalog.ExecMutex.Lock()
	defer catalog.ExecMutex.Unlock()

	if err := cat.AlterColumnType(&column, node.ColumnType, true); err != nil {
		return err
	}
	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func validateColumnType(columnType string) error {
	switch columnType {
	case "text":
	case "uuid":
	default:
		return fmt.Errorf("type %q not supported", columnType)
	}
	return nil
}

func parseMainTableName(mainTableName string) (dbx.Table, error) {
	if !strings.HasSuffix(mainTableName, "__") {
		return dbx.Table{}, fmt.Errorf("%q is not a main table name", mainTableName)
	}
	table, err := dbx.ParseTable(mainTableName[0 : len(mainTableName)-2])
	if err != nil {
		return dbx.Table{}, fmt.Errorf("%q is not a valid table name", mainTableName)
	}
	return table, nil
}
