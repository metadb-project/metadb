package libpq

import (
	"context"
	"fmt"
	"net"
	"syscall"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/parser"
)

func Listen(host string, port string, db *dbx.DB, mdbVersion string) {
	// var h string
	// if host == "" {
	// 	h = "127.0.0.1"
	// } else {
	// 	h = host
	// }
	var ln net.Listener
	var err error
	if host == "" {
		addr := "/tmp/.s.PGSQL." + port
		log.Info("listening on Unix socket \"%s\"", addr)
		_ = syscall.Unlink(addr)
		ln, err = net.Listen("unix", addr)
	} else {
		log.Info("listening on address \"%s\", port %s", host, port)
		ln, err = net.Listen("tcp", net.JoinHostPort(host, port))
	}
	if err != nil {
		// TODO handle error
		_ = err
	}
	log.Info("server is ready to accept connections")
	for {
		var conn net.Conn
		if conn, err = ln.Accept(); err != nil {
			// TODO handle error
			_ = err
		}
		var backend *pgproto3.Backend
		backend = pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
		log.Trace("connection received: %s", conn.RemoteAddr().String())
		go serve(conn, backend, db, mdbVersion)
	}
}

func serve(conn net.Conn, backend *pgproto3.Backend, db *dbx.DB, mdbVersion string) {
	dbconn, err := db.Connect()
	if err != nil {
		// TODO handle error
		log.Info("%v", err)
		return
	}
	// TODO Close

	if err = startup(conn, backend); err != nil {
		// TODO handle error
		log.Info("%v", err)
		return
	}
	for {
		var msg pgproto3.FrontendMessage
		if msg, err = backend.Receive(); err != nil {
			// TODO handle error
			_ = err
			return
		}
		switch m := msg.(type) {
		case *pgproto3.Parse:
			log.Info("*pgproto3.Parse: not yet implemented")
		case *pgproto3.Query:
			if err = processQuery(conn, m, dbconn, mdbVersion); err != nil {
				log.Info("%v", err)
				return
			}
		case *pgproto3.Sync:
			continue
		case *pgproto3.Terminate:
			return
		default:
			log.Info("unknown message: %v", msg)
			// TODO handle error
			_ = err
			return
		}
	}
}

func startup(conn net.Conn, backend *pgproto3.Backend) error {
	var msg pgproto3.FrontendMessage
	var err error
	if msg, err = backend.ReceiveStartupMessage(); err != nil {
		// TODO handle error
		return err
	}
	switch m := msg.(type) {
	case *pgproto3.SSLRequest:
		if _, err = conn.Write([]byte("N")); err != nil {
			return err
		}
		if err = startup(conn, backend); err != nil {
			return err
		}
		return nil
	case *pgproto3.StartupMessage:
		if err = handleStartup(conn, m); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown message: %v", msg)
	}
}

func processQuery(conn net.Conn, query *pgproto3.Query, dbconn *pgx.Conn, mdbVersion string) error {
	var node ast.Node
	var err error
	var pass bool
	var e string
	if node, err, pass = parser.Parse(query.String); err != nil {
		e = err.Error()
	}
	log.Trace("query received: query=%q node=%#v err=%q pass=%v\n", query.String, node, e, pass)
	if pass {
		err = passthroughQuery(conn, query, dbconn)
		if err != nil {
			return write(conn, encode(nil, []pgproto3.Message{
				&pgproto3.ErrorResponse{Message: "ERROR:  " + err.Error()},
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
			}))
		}
		return nil
	}
	if err != nil {
		return write(conn, encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Message: "ERROR:  " + err.Error()},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}))
	}
	switch n := node.(type) {
	case *ast.CreateServerStmt:
		// return createServer(conn, query)
		return version(conn, query, "11111111")
	case *ast.SelectStmt:
		if n.Fn == "version" {
			return version(conn, query, mdbVersion)
		}
		return write(conn, encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Message: "ERROR:  function " + n.Fn + "() does not exist"},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}))
	}
	return write(conn, encode(nil, []pgproto3.Message{
		&pgproto3.ErrorResponse{Message: "ERROR:  syntax error"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	}))
}

func handleStartup(conn net.Conn, msg *pgproto3.StartupMessage) error {
	return write(conn, encode(nil, []pgproto3.Message{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "14.3.0"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	}))
}

func passthroughQuery(conn net.Conn, query *pgproto3.Query, dbconn *pgx.Conn) error {
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
	var cols []pgproto3.FieldDescription = rows.FieldDescriptions()
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

func encodeFieldDesc(buffer []byte, cols []pgproto3.FieldDescription) []byte {
	var desc pgproto3.RowDescription
	var col pgproto3.FieldDescription
	for _, col = range cols {
		var f = pgproto3.FieldDescription{
			Name:                 []byte(col.Name),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          pgtype.TextOID,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		}
		desc.Fields = append(desc.Fields, f)
	}
	return desc.Encode(buffer)
}

func encodeRow(buffer []byte, rows pgx.Rows, cols []pgproto3.FieldDescription) ([]byte, error) {
	var err error
	var numCols = len(cols)
	var dest = make([]any, numCols)
	var vals = make([]any, numCols)
	var i int
	for i = range dest {
		dest[i] = &(vals[i])
	}
	if err = rows.Scan(dest...); err != nil {
		return nil, err
	}
	var row = pgproto3.DataRow{Values: make([][]byte, numCols)}
	var a any
	for i, a = range vals {
		switch v := a.(type) {
		case []byte:
			row.Values[i] = v
		default:
			row.Values[i] = []byte(fmt.Sprintf("%v", v))
		}
	}
	return row.Encode(buffer), nil
}

func version(conn net.Conn, query *pgproto3.Query, mdbVersion string) error {
	var b []byte = encode(nil, []pgproto3.Message{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("version"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		}},
		&pgproto3.DataRow{Values: [][]byte{[]byte("Metadb " + mdbVersion)}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
}

func encode(buffer []byte, messages []pgproto3.Message) []byte {
	if messages == nil || len(messages) == 0 {
		return make([]byte, 0)
	}
	var m pgproto3.Message
	for _, m = range messages {
		buffer = m.Encode(buffer)
	}
	return buffer
}

func write(conn net.Conn, buffer []byte) error {
	if buffer == nil || len(buffer) == 0 {
		return nil
	}
	var err error
	if _, err = conn.Write(buffer); err != nil {
		return err
	}
	return nil
}
