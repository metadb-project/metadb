package libpq

import (
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
)

func alterSystem(conn net.Conn, node *ast.AlterSystemStmt, cat *catalog.Catalog) error {
	if !cat.IsConfigParameterValid(node.ConfigParameter) {
		return fmt.Errorf("unrecognized configuration parameter %q", node.ConfigParameter)
	}

	if err := cat.SetConfig(node.ConfigParameter, node.Value); err != nil {
		return err
	}

	switch node.ConfigParameter {
	case "kafka_sync_concurrency":
		_ = writeEncoded(conn, []pgproto3.Message{
			&pgproto3.NoticeResponse{
				Severity: "INFO",
				Message:  "restart server for changes in \"" + node.ConfigParameter + "\" to take effect",
			},
		})
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("ALTER SYSTEM")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}
