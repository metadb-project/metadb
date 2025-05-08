package libpq

import (
	"context"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/acl"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func deregisterUser(conn net.Conn, node *ast.DeregisterUserStmt, db *dbx.DB, dc *pgx.Conn) error {
	exists, err := catalog.DatabaseUserExists(dc, node.UserName)
	if err != nil {
		return fmt.Errorf("selecting user: %w", err)
	}
	if !exists {
		return fmt.Errorf("user %q does not exist", node.UserName)
	}

	if err := deregister(db, dc, node.UserName, db.DBName); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("DEREGISTER USER")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func deregister(db *dbx.DB, dc *pgx.Conn, user, dbname string) error {

	if err := acl.RevokeAllFromUser(dc, user); err != nil {
		return err
	}

	if _, err := dc.Exec(context.TODO(), "REVOKE CREATE, CONNECT, TEMPORARY ON DATABASE "+dbname+" FROM "+user); err != nil {
		return util.PGErr(err)
	}
	if _, err := dc.Exec(context.TODO(), "DELETE FROM metadb.auth WHERE username=$1", user); err != nil {
		return util.PGErr(err)
	}

	_, _ = dc.Exec(context.TODO(), "REVOKE ALL ON SCHEMA "+user+" FROM "+user)
	_, _ = dc.Exec(context.TODO(), "REVOKE ALL ON TABLES IN SCHEMA "+user+" FROM "+user)

	dcsuper, err := db.ConnectSuper()
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer dbx.Close(dcsuper)
	if _, err = dcsuper.Exec(context.TODO(), "REVOKE USAGE ON SCHEMA public FROM "+user); err != nil {
		return util.PGErr(err)
	}

	return nil
}
