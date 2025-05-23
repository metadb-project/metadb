package libpq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dberr"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/parser"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/tools"
)

var extraManagedSchemas = []string{"folio_derived", "reshare_derived"}
var extraManagedTables = []string{"folio_source_record.marc__t"}

func Listen(cat *catalog.Catalog, host string, port string, db *dbx.DB, sources *[]*sysdb.SourceConnector) {
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
		log.Debug("listening on address %q, port %s", host, port)
		ln, err = net.Listen("tcp", net.JoinHostPort(host, port))
	}
	if err != nil {
		// TODO handle error
		_ = err
	}
	log.Debug("server is ready to accept connections")
	for {
		var conn net.Conn
		if conn, err = ln.Accept(); err != nil {
			// TODO handle error
			_ = err
		}
		backend := pgproto3.NewBackend(conn, conn)
		//log.Trace("connection received: %s", conn.RemoteAddr().String())
		log.Trace("connection received") // domain socket
		go serve(cat, conn, backend, db, sources)
	}
}

func serve(cat *catalog.Catalog, conn net.Conn, backend *pgproto3.Backend, db *dbx.DB, sources *[]*sysdb.SourceConnector) {
	//log.Trace("connected to database")
	// TODO Close

	var err error
	if err = startup(conn, backend); err != nil {
		// errw := write(conn, encode(nil, []pgproto3.Message{
		// 	&pgproto3.ErrorResponse{Message: err.Error()},
		// 	&pgproto3.ReadyForQuery{TxStatus: 'I'},
		// }))
		buffer, erre := encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Message: err.Error()},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		})
		if erre != nil {
			log.Info("%v", erre)
		}
		errw := write(conn, buffer)
		log.Info("connection from address %q: %v", conn.RemoteAddr(), err)
		if errw != nil {
			log.Info("%v", errw)
		}
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
			// Extended query
			if err = processParse(cat, conn, backend, m, db, sources); err != nil {
				log.Info("%v", err)
				return
			}
			//log.Info("*pgproto3.Parse: not yet implemented")
		case *pgproto3.Query:
			if err = processQuery(cat, conn, m.String, nil, db, sources); err != nil {
				log.Info("%v", err)
				return
			}
		case *pgproto3.Sync:
			continue
		case *pgproto3.Terminate:
			return
		default:
			log.Info("unknown message: %#v", msg)
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

func processParse(cat *catalog.Catalog, conn net.Conn, backend *pgproto3.Backend, parse *pgproto3.Parse, db *dbx.DB, sources *[]*sysdb.SourceConnector) error {
	query := parse.Query
	log.Trace("prepared statement: %s", query)

	dc, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer dbx.Close(dc)

	// Prepare the query.
	stmt, err := dc.Prepare(context.TODO(), parse.Name, query)
	if err != nil {
		return fmt.Errorf("preparing extended query: %w", err)
	}

	//var rows pgx.Rows
	//var cols []pgconn.FieldDescription
	//var binds []interface{}
	//exec := func() (err error) {
	//	if rows != nil {
	//		return nil
	//	}
	//	if rows, err = dc.Query(context.TODO(), binds...); err != nil {
	//		return fmt.Errorf("query: %w", err)
	//	}
	//	cols = rows.FieldDescriptions()
	//	return nil
	//}

	args := make([]any, 0)
	_ = args
	for {
		msg, err := backend.Receive()
		if err != nil {
			return fmt.Errorf("unexpected message in extended query: %w", err)
		}

		_ = stmt
		switch m := msg.(type) {
		case *pgproto3.Bind:
			args = make([]any, 0)
			for _, p := range m.Parameters {
				args = append(args, string(p))
			}

		case *pgproto3.Describe:
			return fmt.Errorf("pgproto3.Describe not yet implemented")
			//if err := exec(); err != nil {
			//	return fmt.Errorf("exec: %w", err)
			//}
			//if _, err := c.Write(toRowDescription(cols).Encode(nil)); err != nil {
			//	return err
			//}

		case *pgproto3.Execute:
			//func processQuery(conn net.Conn, query string, db *dbx.DB, dc *pgx.Conn, sources *[]*sysdb.SourceConnector) error {
			err := processQuery(cat, conn, query, args, db, sources)
			//err := proxySelect(conn, query, args, dc)
			if err != nil {
				return fmt.Errorf("executing prepared statement: %w", err)
			}

			//// TODO: Send pgproto3.ParseComplete?
			//if err := exec(); err != nil {
			//	return fmt.Errorf("exec: %w", err)
			//}
			//
			//var buf []byte
			//for rows.Next() {
			//	row, err := scanRow(rows, cols)
			//	if err != nil {
			//		return fmt.Errorf("scan: %w", err)
			//	}
			//	buf = row.Encode(buf)
			//}
			//if err := rows.Err(); err != nil {
			//	return fmt.Errorf("rows: %w", err)
			//}
			//
			//// Mark command complete and ready for next query.
			//buf = (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(buf)
			//buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			//_, err := c.Write(buf)
			//return err

		case *pgproto3.Sync:
			// NOP

		default:
			return fmt.Errorf("unexpected message during parse: %#v", msg)
		}
	}
}

func processQuery(cat *catalog.Catalog, conn net.Conn, query string, args []any, db *dbx.DB, sources *[]*sysdb.SourceConnector) error {
	dc, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer dbx.Close(dc)

	var e string
	node, err, pass := parser.Parse(query)
	if err != nil {
		e = err.Error()
	}
	log.Trace("query received: query=%q node=%#v err=%q pass=%v\n", query, node, e, pass)
	if pass {
		err = proxyQuery(conn, query, args, node, dc)
		if err != nil {
			buffer, erre := encode(nil, []pgproto3.Message{
				&pgproto3.ErrorResponse{Message: "ERROR:  " + err.Error()},
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
			})
			if erre != nil {
				return fmt.Errorf("process query: passthrough: error response: %v", erre)
			}
			return write(conn, buffer)
		}
		return nil
	}
	if err != nil {
		buffer, erre := encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Message: "ERROR:  " + err.Error()},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		})
		if erre != nil {
			return fmt.Errorf("process query: error response: %v", erre)
		}
		return write(conn, buffer)
	}
	switch n := node.(type) {
	case *ast.AlterSystemStmt:
		err = alterSystem(conn, n, cat)
	case *ast.DeregisterUserStmt:
		err = deregisterUser(conn, n, db, dc)
	case *ast.RegisterUserStmt:
		err = registerUser(conn, n, db, dc)
	case *ast.CreateDataSourceStmt:
		err = createDataSource(conn, n, dc)
	case *ast.CreateDataMappingStmt:
		err = createDataMapping(conn, n, cat)
	case *ast.AlterTableStmt:
		err = alterTable(conn, n, dc)
	case *ast.AlterDataSourceStmt:
		err = alterDataSource(conn, n, dc)
	case *ast.CreateUserStmt:
		err = createUser(conn, n, db, dc)
	case *ast.DropUserStmt:
		err = dropUser(conn, n, db, dc)
	case *ast.DropDataSourceStmt:
		err = dropDataSource(conn, n, dc)
	case *ast.AuthorizeStmt:
		err = fmt.Errorf("AUTHORIZE is no longer supported")
	case *ast.DeauthorizeStmt:
		err = fmt.Errorf("DEAUTHORIZE is no longer supported")
	case *ast.GrantAccessOnAllStmt:
		err = grantAccessOnAll(conn, n, dc)
	case *ast.GrantAccessOnFunctionStmt:
		err = grantAccessOnFunction(conn, n, dc)
	case *ast.GrantAccessOnTableStmt:
		err = grantAccessOnTable(conn, n, dc)
	case *ast.RevokeAccessOnAllStmt:
		err = revokeAccessOnAll(conn, n, dc)
	case *ast.RevokeAccessOnFunctionStmt:
		err = revokeAccessOnFunction(conn, n, dc)
	case *ast.RevokeAccessOnTableStmt:
		err = revokeAccessOnTable(conn, n, dc)
	case *ast.CreateDataOriginStmt:
		err = createDataOrigin(conn, n, dc, cat)
	case *ast.ListStmt:
		err = list(conn, n, dc, sources)
	case *ast.RefreshInferredColumnTypesStmt:
		err = refreshInferredColumnTypesStmt(conn, dc)
	case *ast.VerifyConsistencyStmt:
		err = verifyConsistencyStmt(conn, dc)
	case *ast.CreateSchemaForUserStmt:
		err = createSchemaForUser(conn, n, dc)
	//case *ast.SelectStmt:
	//	if n.Fn == "version" {
	//		return version(conn, query)
	//	}
	//	return write(conn, encode(nil, []pgproto3.Message{
	//		&pgproto3.ErrorResponse{Message: "ERROR:  function " + n.Fn + "() does not exist"},
	//		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	//	}))
	default:
		buffer, erre := encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Message: "ERROR:  syntax error"},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		})
		if erre != nil {
			return fmt.Errorf("process query: syntax error response: %v", erre)
		}
		return write(conn, buffer)
	}
	if err != nil {
		hint := ""
		errs, ok := err.(*dberr.Error)
		if ok {
			hint = errs.Hint
		}
		_ = hint // suppress hints until they also can be returned for proxied queries
		buffer, erre := encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Severity: "ERROR", Message: err.Error(), Hint: "" /*hint*/},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		})
		if erre != nil {
			return fmt.Errorf("process query: command error response: %v", erre)
		}
		return write(conn, buffer)
	}
	return nil
}

func handleStartup(conn net.Conn, msg *pgproto3.StartupMessage) error {
	if msg.ProtocolVersion != 0x30000 {
		return fmt.Errorf("startup: unknown protocol version \"%#x\"", msg.ProtocolVersion)
	}
	if msg.Parameters["application_name"] != "psql" {
		return fmt.Errorf("startup: unsupported application %q", msg.Parameters["application_name"])
	}
	if msg.Parameters["database"] != "metadb" {
		return fmt.Errorf("startup: unsupported database name %q (use \"-d metadb\")", msg.Parameters["database"])
	}
	buffer, erre := encode(nil, []pgproto3.Message{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "15.3.0"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	if erre != nil {
		return fmt.Errorf("startup: %v", erre)
	}
	return write(conn, buffer)
}

func encodeFieldDesc(buffer []byte, cols []pgconn.FieldDescription) ([]byte, error) {
	var desc pgproto3.RowDescription
	var col pgconn.FieldDescription
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

func encodeRow(buffer []byte, rows pgx.Rows, cols []pgconn.FieldDescription) ([]byte, error) {
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
		if a == nil {
			row.Values[i] = []byte("")
			continue
		}
		switch v := a.(type) {
		case []byte:
			row.Values[i] = v
		default:
			row.Values[i] = []byte(fmt.Sprintf("%v", v))
		}
	}
	return row.Encode(buffer)
}

func alterTable(conn net.Conn, node *ast.AlterTableStmt, dc *pgx.Conn) error {
	// The only type currently supported is uuid.
	columnType := strings.ToLower(node.Cmd.ColumnType)
	if columnType != "uuid" {
		return fmt.Errorf("converting to type %q not supported", columnType)
	}

	// Ensure the table name is a main table, and parse it.
	if !strings.HasSuffix(node.TableName, "__") {
		return fmt.Errorf("%q is not a main table name", node.TableName)
	}
	table, err := dbx.ParseTable(node.TableName[0 : len(node.TableName)-2])
	if err != nil {
		return fmt.Errorf("%q is not a valid table name", node.TableName)
	}

	// Ensure the table is in the catalog.
	q := "SELECT 1 FROM metadb.base_table WHERE schema_name=$1 AND table_name=$2"
	var i int64
	err = dc.QueryRow(context.TODO(), q, table.Schema, table.Table).Scan(&i)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("table %q does not exist in a data source", node.TableName)
	case err != nil:
		return fmt.Errorf("looking up table %q: %v", node.TableName, err)
	default:
		// NOP: table found.
	}

	// Ensure the table has the requested column.
	q = `SELECT t.typname
    FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace=n.oid
        JOIN pg_attribute a ON a.attrelid=c.oid
        JOIN pg_type t ON t.oid=a.atttypid
    WHERE n.nspname=$1 AND c.relname=$2 AND a.attname=$3`
	var t string
	err = dc.QueryRow(context.TODO(), q, table.Schema, table.Table+"__", node.Cmd.ColumnName).Scan(&t)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("column %q of table %q does not exist", node.Cmd.ColumnName, node.TableName)
	case err != nil:
		return fmt.Errorf("looking up column %q in table %q: %v", node.Cmd.ColumnName, node.TableName, err)
	default:
		// NOP: column found.
	}

	if t != "uuid" { // No need to do anything if the type is already uuid.
		// Convert the column type.
		q = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"%s\" TYPE %s USING \"%s\"::%s",
			table.MainSQL(), node.Cmd.ColumnName, node.Cmd.ColumnType, node.Cmd.ColumnName, node.Cmd.ColumnType)
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return errors.New(strings.TrimPrefix(err.Error(), "ERROR: "))
		}
		// Create an index on a uuid column.
		q = fmt.Sprintf("CREATE INDEX ON %s (\"%s\")", table.MainSQL(), node.Cmd.ColumnName)
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return errors.New(strings.TrimPrefix(err.Error(), "ERROR: "))
		}
	}
	_ = writeEncoded(conn, []pgproto3.Message{
		&pgproto3.NoticeResponse{Severity: "INFO", Message: "restart server for table changes to take full effect"},
	})
	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func dropDataSource(conn net.Conn, node *ast.DropDataSourceStmt, dc *pgx.Conn) error {
	exists, err := sourceExists(dc, node.DataSourceName)
	if err != nil {
		return fmt.Errorf("selecting data source: %w", err)
	}
	if !exists {
		return fmt.Errorf("data source %q does not exist", node.DataSourceName)
	}

	q := "DELETE FROM metadb.source WHERE name='" + node.DataSourceName + "'"
	_, err = dc.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("deleting data source %q", node.DataSourceName)
	}
	_ = writeEncoded(conn, []pgproto3.Message{
		&pgproto3.NoticeResponse{Severity: "INFO", Message: "restart server for data source changes to take effect"},
	})
	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("DROP DATA SOURCE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func updateSource(dc *pgx.Conn, sourceName, optionName, valueText string) error {
	q := "UPDATE metadb.source SET " + optionName + "=" + valueText + " WHERE name='" + sourceName + "'"
	_, err := dc.Exec(context.TODO(), q)
	return err
}

func checkOptionDuplicates(options []ast.Option) error {
	m := make(map[string]bool)
	for _, opt := range options {
		name := strings.ToLower(opt.Name)
		if m[name] {
			return fmt.Errorf("option %q provided more than once", opt.Name)
		}
		m[name] = true
	}
	return nil
}

func createSchemaForUser(conn net.Conn, node *ast.CreateSchemaForUserStmt, dc *pgx.Conn) error {
	reg, err := catalog.UserRegistered(dc, node.UserName)
	if err != nil {
		return fmt.Errorf("selecting user: %v", err)
	}
	if !reg {
		return fmt.Errorf("%q is not a registered user", node.UserName)
	}

	if err := createUserSchema(dc, node.UserName); err != nil {
		return err
	}
	if err := grantCreateOnUserSchema(dc, node.UserName); err != nil {
		return err
	}
	if err := grantUsageOnUserSchema(dc, node.UserName); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("CREATE SCHEMA")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func sourceExists(dc *pgx.Conn, sourceName string) (bool, error) {
	q := "SELECT 1 FROM metadb.source WHERE name=$1"
	var i int64
	err := dc.QueryRow(context.TODO(), q, sourceName).Scan(&i)
	switch {
	case err == pgx.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func refreshInferredColumnTypesStmt(conn net.Conn, dc *pgx.Conn) error {
	err := tools.RefreshInferredColumnTypes(dc, func(msg string) {
		_ = writeEncoded(conn, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "INFO",
			Message: msg},
		})
	})
	if err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("REFRESH INFERRED COLUMN TYPES")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func verifyConsistencyStmt(conn net.Conn, dc *pgx.Conn) error {
	err := tools.VerifyConsistency(dc, func(msg string) {
		_ = writeEncoded(conn, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "INFO",
			Message: msg},
		})
	})
	if err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("VERIFY CONSISTENCY")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

//func version(conn net.Conn, query *pgproto3.Query, mdbVersion string) error {
//	var b []byte = encode(nil, []pgproto3.Message{
//		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
//			{
//				Name:                 []byte("version"),
//				TableOID:             0,
//				TableAttributeNumber: 0,
//				DataTypeOID:          25,
//				DataTypeSize:         -1,
//				TypeModifier:         -1,
//				Format:               0,
//			},
//		}},
//		&pgproto3.DataRow{Values: [][]byte{[]byte(util.MetadbVersionString())}},
//		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
//		&pgproto3.ReadyForQuery{TxStatus: 'I'},
//	})
//	return write(conn, b)
//}

func writeEncoded(conn net.Conn, messages []pgproto3.Message) error {
	buffer, erre := encode(nil, messages)
	if erre != nil {
		return erre
	}
	errc := write(conn, buffer)
	if errc != nil {
		return errc
	}
	return nil
}

func encode(buffer []byte, messages []pgproto3.Message) ([]byte, error) {
	if len(messages) == 0 {
		return make([]byte, 0), nil
	}
	var m pgproto3.Message
	var err error
	for _, m = range messages {
		buffer, err = m.Encode(buffer)
		if err != nil {
			return nil, fmt.Errorf("encode: %w", err)
		}
	}
	return buffer, nil
}

func write(conn net.Conn, buffer []byte) error {
	if len(buffer) == 0 {
		return nil
	}
	if _, err := conn.Write(buffer); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}
