package libpq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func purgeDataDropTable(conn net.Conn, node *ast.PurgeDataDropTableStmt, db *dbx.DB, dc *pgx.Conn) error {
	// ensure table name is a main table and parse it
	if !strings.HasSuffix(node.TableName, "__") {
		return fmt.Errorf("%q is not a main table name", node.TableName)
	}
	table, err := dbx.ParseTable(node.TableName[0 : len(node.TableName)-2])
	if err != nil {
		return fmt.Errorf("%q is not a valid table name", node.TableName)
	}

	// ensure table is in catalog
	q := "SELECT 1 FROM metadb.base_table WHERE schema_name=$1 AND table_name=$2"
	var i int64
	err = dc.QueryRow(context.TODO(), q, table.Schema, table.Table).Scan(&i)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("table %q does not exist in a data source", node.TableName)
	case err != nil:
		return fmt.Errorf("looking up table %q: %v", node.TableName, err)
	default:
		// nop - table found
	}

	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return util.PGErr(err)
	}
	defer dbx.Rollback(tx)

	// delete from catalog
	q = "DELETE FROM metadb.base_table WHERE schema_name=$1 AND table_name=$2"
	if _, err = tx.Exec(context.TODO(), q, table.Schema, table.Table); err != nil {
		return util.PGErr(err)
	}
	q = "DELETE FROM metadb.acl WHERE schema_name=$1 AND object_name=$2 AND object_type='t'"
	if _, err = tx.Exec(context.TODO(), q, table.Schema, table.Table); err != nil {
		return util.PGErr(err)
	}

	// drop table
	q = "DROP TABLE \"" + table.Schema + "\".\"" + table.Table + "__\""
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return util.PGErr(err)
	}
	q = "DROP TABLE \"" + table.Schema + "\".\"zzz___" + table.Table + "___sync\""
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return util.PGErr(err)
	}

	if err = tx.Commit(context.TODO()); err != nil {
		return util.PGErr(err)
	}
	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("PURGE DATA")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}
