package libpq

import (
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
)

var schemaTableRegexp = regexp.MustCompile(`^[_a-z][0-9_a-z]*[.][_a-z][0-9_a-z]*[_][_]$`)
var columnRegexp = regexp.MustCompile(`^[_a-z][0-9_a-z]*$`)
var identifierRegexp = regexp.MustCompile(`^[a-z][0-9a-z]*$`)

func createDataMapping(conn net.Conn, node *ast.CreateDataMappingStmt, cat *catalog.Catalog) error {
	// The only mapping type currently supported is json.
	if node.TypeName != "json" {
		return fmt.Errorf("mapping type %q not supported", node.TypeName)
	}

	// Validate schema.table name.
	if node.TableName == "" || !schemaTableRegexp.MatchString(node.TableName) {
		return fmt.Errorf("table name %q is invalid", node.TableName)
	}
	// Parse the schema.table name.
	table, err := dbx.ParseTable(node.TableName[0 : len(node.TableName)-2])
	if err != nil {
		return fmt.Errorf("%q is not a valid table name", node.TableName)
	}
	// Validate table name length.
	if len(table.Schema) > 63 || len(table.Table) > 63 {
		return fmt.Errorf("%q is not a valid table name", node.TableName)
	}

	// Validate column name.
	if node.ColumnName == "" || !columnRegexp.MatchString(node.ColumnName) {
		return fmt.Errorf("column name %q is invalid", node.ColumnName)
	}
	if len(node.ColumnName) > 63 {
		return fmt.Errorf("%q is not a valid column name", node.ColumnName)
	}

	// Validate path.
	p := strings.Split(node.Path, ".")
	if len(p) < 1 || p[0] != "$" {
		return fmt.Errorf("path %q is invalid", node.Path)
	}
	for i := 1; i < len(p); i++ {
		if p[i] == "" {
			return fmt.Errorf("path %q is invalid", node.Path)
		}
	}

	// Validate target identifier.
	if node.TargetIdentifier == "" || !identifierRegexp.MatchString(node.TargetIdentifier) {
		return fmt.Errorf("target identifier %q is invalid", node.TargetIdentifier)
	}
	const maxTargetIdentifierLen = 16
	if len(node.TargetIdentifier) > maxTargetIdentifierLen {
		return fmt.Errorf("target identifier %q is too long (maximum length %d characters)", node.TargetIdentifier, maxTargetIdentifierLen)
	}

	if err := cat.DefineJSONMapping(table.Schema, table.Table, node.ColumnName, node.Path, node.TargetIdentifier); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("CREATE DATA MAPPING")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}
