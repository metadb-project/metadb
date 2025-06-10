package libpq

import (
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
)

func dropDataMapping(conn net.Conn, node *ast.DropDataMappingStmt, cat *catalog.Catalog) error {
	// Parse the schema.table name.
	table, err := dbx.ParseTable(node.TableName[0 : len(node.TableName)-2])
	if err != nil {
		return fmt.Errorf("%q is not a valid table name", node.TableName)
	}

	if err := cat.RemoveJSONMapping(table.Schema, table.Table, node.ColumnName, node.Path); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("DROP DATA MAPPING")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}
