package libpq

import (
	"context"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
)

func createDataOrigin(conn net.Conn, node *ast.CreateDataOriginStmt, dc *pgx.Conn, cat *catalog.Catalog) error {
	if len(node.OriginName) > 63 {
		return fmt.Errorf("data origin name %q too long", node.OriginName)
	}

	exists, err := originExists(dc, node.OriginName)
	if err != nil {
		return fmt.Errorf("selecting data origin: %w", err)
	}
	if exists {
		return fmt.Errorf("data origin %q already exists", node.OriginName)
	}

	if err := cat.DefineOrigin(node.OriginName); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("CREATE DATA ORIGIN")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func originExists(dc *pgx.Conn, originName string) (bool, error) {
	q := "SELECT 1 FROM metadb.origin WHERE name=$1"
	var i int64
	err := dc.QueryRow(context.TODO(), q, originName).Scan(&i)
	switch {
	case err == pgx.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}
