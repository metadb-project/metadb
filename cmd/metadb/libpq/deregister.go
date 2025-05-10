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

	dcsuper, err := db.ConnectSuper()
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer dbx.Close(dcsuper)

	if err := deregister(db, dc, dcsuper, node.UserName, db.DBName); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("DEREGISTER USER")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func deregister(db *dbx.DB, dc, dcsuper *pgx.Conn, user, dbname string) error {

	if err := acl.RevokeAllFromUser(dc, user); err != nil {
		return err
	}

	var schemas []string
	schemas, err := schemasWithUserPrivileges(dc, user)
	if err != nil {
		return fmt.Errorf("reading schemas for user %q: %w", user, err)
	}
	batch := pgx.Batch{}
	for i := range schemas {
		batch.Queue("REVOKE ALL ON SCHEMA " + schemas[i] + " FROM " + user)
	}
	if err = dc.SendBatch(context.TODO(), &batch).Close(); err != nil {
		return fmt.Errorf("removing schema privleges for user %q: %w", user, err)
	}

	var tables []dbx.Table
	tables, err = tablesOwnedByUser(dc, user)
	if err != nil {
		return fmt.Errorf("reading tables for user %q: %w", user, err)
	}
	batch = pgx.Batch{}
	for i := range tables {
		batch.Queue("ALTER TABLE " + tables[i].Schema + "." + tables[i].Table + " OWNER TO " + db.User)
	}
	if err = dcsuper.SendBatch(context.TODO(), &batch).Close(); err != nil {
		return fmt.Errorf("removing table ownership for user %q: %w", user, err)
	}

	var functions []dbx.Function
	functions, err = functionsOwnedByUser(dc, user)
	if err != nil {
		return fmt.Errorf("reading functions for user %q: %w", user, err)
	}
	batch = pgx.Batch{}
	for i := range functions {
		batch.Queue("ALTER FUNCTION " + functions[i].Schema + "." + functions[i].Function + " OWNER TO " + db.User)
	}
	if err = dcsuper.SendBatch(context.TODO(), &batch).Close(); err != nil {
		return fmt.Errorf("removing function ownership for user %q: %w", user, err)
	}

	_, _ = dc.Exec(context.TODO(), "REVOKE ALL ON TABLES IN SCHEMA "+user+" FROM "+user)
	_, _ = dc.Exec(context.TODO(), "REVOKE ALL ON SCHEMA "+user+" FROM "+user)

	if _, err = dc.Exec(context.TODO(), "REVOKE CREATE, CONNECT, TEMPORARY ON DATABASE "+dbname+" FROM "+user); err != nil {
		return util.PGErr(err)
	}
	if _, err = dc.Exec(context.TODO(), "DELETE FROM metadb.auth WHERE username=$1", user); err != nil {
		return util.PGErr(err)
	}

	if _, err = dcsuper.Exec(context.TODO(), "REVOKE USAGE ON SCHEMA public FROM "+user); err != nil {
		return util.PGErr(err)
	}

	return nil
}
