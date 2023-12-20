package libpq

import (
	"context"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/parser"
)

func proxyQuery(conn net.Conn, query string, args []any, node ast.Node, dc *pgx.Conn) error {
	switch node.(type) {
	case *ast.SelectStmt:
		return proxySelect(conn, query, args, dc)
		//case *ast.CreateUserStmt:
		//	return createUser(conn, query, n, db)
	}

	return write(conn, encode(nil, []pgproto3.Message{
		&pgproto3.ErrorResponse{Severity: "ERROR", Message: "syntax error"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	}))

	//ctag, err := dc.Exec(context.TODO(), query)
	//if err != nil {
	//	return errors.New(strings.TrimLeft(strings.TrimPrefix(err.Error(), "ERROR:"), " "))
	//}
	//b := encode(nil, []pgproto3.Message{
	//	&pgproto3.CommandComplete{CommandTag: []byte(ctag.String())},
	//	&pgproto3.ReadyForQuery{TxStatus: 'I'},
	//})
	//return write(conn, b)
}

func proxySelect(conn net.Conn, query string, args []any, dbconn *pgx.Conn) error {
	var err error
	var rows pgx.Rows
	if rows, err = dbconn.Query(context.TODO(), query, args...); err != nil {
		var ok bool
		var e *pgconn.PgError
		if e, ok = err.(*pgconn.PgError); !ok {
			panic("passthroughQuery(): casting error to *pgconn.PgError")
		}
		var s = e.Severity + ":  " + e.Message + "\n" + parser.WriteErrorContext(query, int(e.Position-1))
		return write(conn, encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Message: s},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}))
	}
	defer rows.Close()
	var cols = rows.FieldDescriptions()
	var b = encodeFieldDesc(nil, cols)
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
