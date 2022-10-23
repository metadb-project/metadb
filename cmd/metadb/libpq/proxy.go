package libpq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/parser"
)

func proxyQuery(conn net.Conn, query *pgproto3.Query, node ast.Node, db *dbx.DB, dc *pgx.Conn) error {
	switch n := node.(type) {
	case *ast.SelectStmt:
		return proxySelect(conn, query, dc)
	case *ast.CreateUserStmt:
		return createUser(conn, query, n, db)
	}

	return write(conn, encode(nil, []pgproto3.Message{
		&pgproto3.ErrorResponse{Severity: "ERROR", Message: "syntax error"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	}))

	//ctag, err := dc.Exec(context.TODO(), query.String)
	//if err != nil {
	//	return errors.New(strings.TrimLeft(strings.TrimPrefix(err.Error(), "ERROR:"), " "))
	//}
	//b := encode(nil, []pgproto3.Message{
	//	&pgproto3.CommandComplete{CommandTag: []byte(ctag.String())},
	//	&pgproto3.ReadyForQuery{TxStatus: 'I'},
	//})
	//return write(conn, b)
}

func createUser(conn net.Conn, query *pgproto3.Query, node *ast.CreateUserStmt, db *dbx.DB) error {
	dc, err := db.ConnectSuper()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	exists, err := userExists(dc, node.UserName)
	if err != nil {
		return fmt.Errorf("selecting role: %v", err)
	}
	if exists {
		_ = write(conn, encode(nil, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "NOTICE",
			Message: fmt.Sprintf("role %q already exists, skipping", node.UserName)},
		}))
	} else {
		_, err = dc.Exec(context.TODO(), query.String)
		if err != nil {
			return errors.New(strings.TrimLeft(strings.TrimPrefix(err.Error(), "ERROR:"), " "))
		}
	}

	q := "CREATE SCHEMA IF NOT EXISTS " + node.UserName
	_, err = dc.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("creating schema %s: %s", node.UserName, err)
	}
	q = "ALTER SCHEMA " + node.UserName + " OWNER TO " + db.User
	_, err = dc.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("setting owner of schema %s: %s", node.UserName, err)
	}
	q = "GRANT CREATE, USAGE ON SCHEMA " + node.UserName + " TO " + node.UserName
	_, err = dc.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("granting privileges on schema %q to role %q: %s", node.UserName, node.UserName, err)
	}

	b := encode(nil, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("CREATE ROLE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
}

func proxySelect(conn net.Conn, query *pgproto3.Query, dbconn *pgx.Conn) error {
	var err error
	var rows pgx.Rows
	if rows, err = dbconn.Query(context.TODO(), query.String); err != nil {
		var ok bool
		var e *pgconn.PgError
		if e, ok = err.(*pgconn.PgError); !ok {
			panic("passthroughQuery(): casting error to *pgconn.PgError")
		}
		var s = e.Severity + ":  " + e.Message + "\n" + parser.WriteErrorContext(query.String, int(e.Position-1))
		return write(conn, encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Message: s},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}))
	}
	defer rows.Close()
	var cols []pgconn.FieldDescription = rows.FieldDescriptions()
	var b []byte = encodeFieldDesc(nil, cols)
	for rows.Next() {
		if b, err = encodeRow(b, rows, cols); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	b = encode(b, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
}
