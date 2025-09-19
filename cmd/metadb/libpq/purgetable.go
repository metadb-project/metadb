package libpq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/acl"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func purgeDataDropTable(conn net.Conn, node *ast.PurgeDataDropTableStmt, db *dbx.DB, dc *pgx.Conn, cat *catalog.Catalog) error {

	for i := range node.TableNames {
		// ensure table name is a main table and parse it
		if !strings.HasSuffix(node.TableNames[i], "__") {
			return fmt.Errorf("%q is not a main table name", node.TableNames[i])
		}
		table, err := dbx.ParseTable(node.TableNames[i][0 : len(node.TableNames[i])-2])
		if err != nil {
			return fmt.Errorf("%q is not a valid table name", node.TableNames[i])
		}
		// ensure table is in catalog
		q := "SELECT 1 FROM metadb.base_table WHERE schema_name=$1 AND table_name=$2"
		var i int64
		err = dc.QueryRow(context.TODO(), q, table.Schema, table.Table).Scan(&i)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("table %q does not exist in a data source", node.TableNames[i])
		case err != nil:
			return fmt.Errorf("looking up table %q: %v", node.TableNames[i], err)
		default:
			// nop - table found
		}
	}

	_ = writeEncoded(conn, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "INFO",
		Message: "waiting for stream processor lock"},
	})

	catalog.ExecMutex.Lock()
	defer catalog.ExecMutex.Unlock()

	tx, err2 := dc.Begin(context.TODO())
	if err2 != nil {
		return util.PGErr(err2)
	}
	defer dbx.Rollback(tx)

	for i := range node.TableNames {
		_ = writeEncoded(conn, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "INFO",
			Message: fmt.Sprintf("dropping %q", node.TableNames[i])},
		})
		table, err := dbx.ParseTable(node.TableNames[i][0 : len(node.TableNames[i])-2])
		if err = acl.RevokeAllOnObject(tx, table.Schema, table.Table, acl.Table); err != nil {
			return err
		}
		if err = cat.DropTable(tx, &table); err != nil {
			return err
		}
	}

	if err2 = tx.Commit(context.TODO()); err2 != nil {
		return util.PGErr(err2)
	}
	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("PURGE DATA")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}
