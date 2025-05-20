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

var identifierRegexp = regexp.MustCompile(`^[a-z][0-9a-z]*$`)

func createDataMapping(conn net.Conn, node *ast.CreateDataMappingStmt, cat *catalog.Catalog) error {
	// The only mapping type currently supported is json.
	if node.TypeName != "json" {
		return fmt.Errorf("mapping type %q not supported", node.TypeName)
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

	// Ensure the table name is a main table, and parse it.
	if !strings.HasSuffix(node.TableName, "__") {
		return fmt.Errorf("%q is not a main table name", node.TableName)
	}
	table, err := dbx.ParseTable(node.TableName[0 : len(node.TableName)-2])
	if err != nil {
		return fmt.Errorf("%q is not a valid table name", node.TableName)
	}
	// Validate table name.
	if len(table.Schema) > 63 || len(table.Table) > 63 {
		return fmt.Errorf("%q is not a valid table name", node.TableName)
	}

	// Validate column name.
	if len(node.ColumnName) > 63 {
		return fmt.Errorf("%q is not a valid column name", node.ColumnName)
	}

	if err := cat.DefineJSONMapping(table.Schema, table.Table, node.ColumnName, node.Path, node.TargetIdentifier); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("CREATE DATA MAPPING")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}
