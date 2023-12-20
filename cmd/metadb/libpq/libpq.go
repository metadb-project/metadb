package libpq

import (
	"context"
	"errors"
	"fmt"
	"github.com/metadb-project/metadb/cmd/metadb/tools"
	"net"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/dberr"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/parser"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
)

func Listen(host string, port string, db *dbx.DB, sources *[]*sysdb.SourceConnector) {
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
		var backend *pgproto3.Backend
		backend = pgproto3.NewBackend(conn, conn)
		//log.Trace("connection received: %s", conn.RemoteAddr().String())
		log.Trace("connection received") // domain socket
		go serve(conn, backend, db, sources)
	}
}

func serve(conn net.Conn, backend *pgproto3.Backend, db *dbx.DB, sources *[]*sysdb.SourceConnector) {
	dbconn, err := db.Connect()
	if err != nil {
		// TODO handle error
		log.Info("%v", err)
		return
	}
	defer dbx.Close(dbconn)
	//log.Trace("connected to database")
	// TODO Close

	if err = startup(conn, backend); err != nil {
		errw := write(conn, encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Message: err.Error()},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}))
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
		log.Trace("*** %#v", msg)

		switch m := msg.(type) {
		case *pgproto3.Parse:
			// Extended query
			if err = processParse(conn, backend, m, db, dbconn, sources); err != nil {
				log.Info("%v", err)
				return
			}
			//log.Info("*pgproto3.Parse: not yet implemented")
		case *pgproto3.Query:
			if err = processQuery(conn, m.String, nil, db, dbconn, sources); err != nil {
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

func processParse(conn net.Conn, backend *pgproto3.Backend, parse *pgproto3.Parse, db *dbx.DB, dc *pgx.Conn, sources *[]*sysdb.SourceConnector) error {
	query := parse.Query
	log.Trace("prepared statement: %s", query)

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
			return fmt.Errorf("unexpected message in extended query: %v", err)
		}
		log.Trace("*** %#v", msg)

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
			//func processQuery(conn net.Conn, query string, db *dbx.DB, dbconn *pgx.Conn, sources *[]*sysdb.SourceConnector) error {
			err := processQuery(conn, query, args, db, dc, sources)
			//err := proxySelect(conn, query, args, dc)
			if err != nil {
				return fmt.Errorf("executing prepared statement: %v", err)
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

func processQuery(conn net.Conn, query string, args []any, db *dbx.DB, dbconn *pgx.Conn, sources *[]*sysdb.SourceConnector) error {
	var e string
	node, err, pass := parser.Parse(query)
	if err != nil {
		e = err.Error()
	}
	log.Trace("query received: query=%q node=%#v err=%q pass=%v\n", query, node, e, pass)
	if pass {
		err = proxyQuery(conn, query, args, node, dbconn)
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
	case *ast.CreateDataSourceStmt:
		err = createDataSource(conn, n, dbconn)
	case *ast.AlterTableStmt:
		err = alterTable(conn, n, dbconn)
	case *ast.AlterDataSourceStmt:
		err = alterDataSource(conn, n, dbconn)
	case *ast.CreateUserStmt:
		err = createUser(conn, n, db, dbconn)
	case *ast.DropDataSourceStmt:
		err = dropDataSource(conn, n, dbconn)
	case *ast.AuthorizeStmt:
		err = authorize(conn, n, dbconn)
	case *ast.CreateDataOriginStmt:
		err = createDataOrigin(conn, n, dbconn)
	case *ast.ListStmt:
		err = list(conn, n, dbconn, sources)
	case *ast.RefreshInferredColumnTypesStmt:
		err = refreshInferredColumnTypesStmt(conn, dbconn)
	case *ast.VerifyConsistencyStmt:
		err = verifyConsistencyStmt(conn, dbconn)
	//case *ast.SelectStmt:
	//	if n.Fn == "version" {
	//		return version(conn, query)
	//	}
	//	return write(conn, encode(nil, []pgproto3.Message{
	//		&pgproto3.ErrorResponse{Message: "ERROR:  function " + n.Fn + "() does not exist"},
	//		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	//	}))
	default:
		return write(conn, encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Message: "ERROR:  syntax error"},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}))
	}
	if err != nil {
		//log.Error("%v: %s", err, query)
		hint := ""
		errs, ok := err.(*dberr.Error)
		if ok {
			hint = errs.Hint
		}
		_ = hint // suppress hints until they also can be returned for proxied queries
		return write(conn, encode(nil, []pgproto3.Message{
			&pgproto3.ErrorResponse{Severity: "ERROR", Message: err.Error(), Hint: "" /*hint*/},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}))
	}
	return nil
}

func handleStartup(conn net.Conn, msg *pgproto3.StartupMessage) error {
	if msg.ProtocolVersion != 0x30000 {
		return fmt.Errorf("unknown protocol version \"%#x\"", msg.ProtocolVersion)
	}
	if msg.Parameters["application_name"] != "psql" {
		return fmt.Errorf("unsupported application %q", msg.Parameters["application_name"])
	}
	if msg.Parameters["database"] != "metadb" {
		return fmt.Errorf("unsupported database name %q (use \"-d metadb\")", msg.Parameters["database"])
	}
	return write(conn, encode(nil, []pgproto3.Message{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "15.3.0"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	}))
}

func encodeFieldDesc(buffer []byte, cols []pgconn.FieldDescription) []byte {
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
		switch v := a.(type) {
		case []byte:
			row.Values[i] = v
		default:
			row.Values[i] = []byte(fmt.Sprintf("%v", v))
		}
	}
	return row.Encode(buffer), nil
}

func list(conn net.Conn, node *ast.ListStmt, dc *pgx.Conn, sources *[]*sysdb.SourceConnector) error {
	switch strings.ToLower(node.Name) {
	case "authorizations":
		return proxySelect(conn, ""+
			"SELECT username,"+
			"       CASE WHEN (NOT dbupdated) THEN 'pending restart'"+
			"            WHEN (tables='.*' AND dbupdated) THEN 'authorized'"+
			"            ELSE 'not authorized'"+
			"       END note"+
			"    FROM metadb.auth", nil, dc)
	case "data_origins":
		return proxySelect(conn, "SELECT name FROM metadb.origin", nil, dc)
	case "data_sources":
		return proxySelect(conn, ""+
			"SELECT name,"+
			"       brokers,"+
			"       security,"+
			"       topics,"+
			"       consumergroup,"+
			"       schemapassfilter,"+
			"       schemastopfilter,"+
			"       trimschemaprefix,"+
			"       addschemaprefix,"+
			"       module"+
			"    FROM metadb.source", nil, dc)
	case "status":
		return listStatus(conn, sources)
	default:
		return fmt.Errorf("unrecognized parameter %q", node.Name)
	}
}

func listStatus(conn net.Conn, sources *[]*sysdb.SourceConnector) error {
	m := []pgproto3.Message{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("type"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
			{
				Name:                 []byte("name"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
			{
				Name:                 []byte("status"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		}},
	}
	for _, s := range *sources {
		m = append(m, &pgproto3.DataRow{Values: [][]byte{
			[]byte("data source"),
			[]byte(s.Name),
			[]byte(s.Status.GetString()),
		}})
	}
	ctag := fmt.Sprintf("SELECT %d", len(*sources))
	m = append(m, &pgproto3.CommandComplete{CommandTag: []byte(ctag)})
	m = append(m, &pgproto3.ReadyForQuery{TxStatus: 'I'})
	b := encode(nil, m)
	return write(conn, b)
}

func createDataSource(conn net.Conn, node *ast.CreateDataSourceStmt, dc *pgx.Conn) error {
	exists, err := sourceExists(dc, node.DataSourceName)
	if err != nil {
		return fmt.Errorf("selecting data source: %v", err)
	}
	if exists {
		return fmt.Errorf("data source %q already exists", node.DataSourceName)
	}

	var count int64
	q := "SELECT count(*) FROM metadb.source"
	err = dc.QueryRow(context.TODO(), q).Scan(&count)
	if err != nil {
		return fmt.Errorf("checking number of configured sources: %v", err)
	}
	if count > 0 {
		return fmt.Errorf("multiple sources not currently supported")
	}

	name := node.DataSourceName
	if node.TypeName != "kafka" {
		return fmt.Errorf("invalid data source type %q", node.TypeName)
	}
	if node.Options == nil {
		// return to client
	}
	src, err := createSourceOptions(node.Options)
	if err != nil {
		return err
	}

	q = "INSERT INTO metadb.source" +
		"(name,brokers,security,topics,consumergroup,schemapassfilter,schemastopfilter,tablestopfilter,trimschemaprefix,addschemaprefix,module,enable)" +
		"VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"
	_, err = dc.Exec(context.TODO(), q,
		name, src.Brokers, src.Security, strings.Join(src.Topics, ","), src.Group,
		strings.Join(src.SchemaPassFilter, ","), strings.Join(src.SchemaStopFilter, ","),
		strings.Join(src.TableStopFilter, ","), src.TrimSchemaPrefix, src.AddSchemaPrefix, src.Module,
		src.Enable)
	if err != nil {
		return fmt.Errorf("writing source configuration: %v", err)
	}

	b := encode(nil, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("CREATE DATA SOURCE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
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

	b := encode(nil, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
}

func alterDataSource(conn net.Conn, node *ast.AlterDataSourceStmt, dc *pgx.Conn) error {
	exists, err := sourceExists(dc, node.DataSourceName)
	if err != nil {
		return fmt.Errorf("selecting data source: %v", err)
	}
	if !exists {
		return fmt.Errorf("data source %q does not exist", node.DataSourceName)
	}

	err = alterSourceOptions(dc, node)
	if err != nil {
		return err
	}

	_ = write(conn, encode(nil, []pgproto3.Message{
		&pgproto3.NoticeResponse{Severity: "INFO", Message: "restart server for data source changes to take effect"},
	}))

	b := encode(nil, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("ALTER DATA SOURCE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
}

func dropDataSource(conn net.Conn, node *ast.DropDataSourceStmt, dc *pgx.Conn) error {
	exists, err := sourceExists(dc, node.DataSourceName)
	if err != nil {
		return fmt.Errorf("selecting data source: %v", err)
	}
	if !exists {
		return fmt.Errorf("data source %q does not exist", node.DataSourceName)
	}

	q := "DELETE FROM metadb.source WHERE name='" + node.DataSourceName + "'"
	_, err = dc.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("deleting data source %q", node.DataSourceName)
	}

	b := encode(nil, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("DROP DATA SOURCE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
}

func alterSourceOptions(dc *pgx.Conn, node *ast.AlterDataSourceStmt) error {
	for _, opt := range node.Options {
		switch opt.Name {
		case "brokers":
			fallthrough
		case "security":
			fallthrough
		case "topics":
			fallthrough
		case "consumergroup":
			fallthrough
		case "schemapassfilter":
			fallthrough
		case "schemastopfilter":
			fallthrough
		case "tablestopfilter":
			fallthrough
		case "trimschemaprefix":
			fallthrough
		case "addschemaprefix":
			fallthrough
		case "module":
			// NOP
		default:
			return &dberr.Error{
				Err: fmt.Errorf("invalid option %q", opt.Name),
				Hint: "Valid options in this context are: " +
					"brokers, security, topics, consumergroup, schemapassfilter, schemastopfilter, tablestopfilter, trimschemaprefix, addschemaprefix, module",
			}
		}
		isnull, err := isSourceOptionNull(dc, node.DataSourceName, opt.Name)
		if err != nil {
			return fmt.Errorf("reading source option: %v", err)
		}
		if opt.Action == "DROP" {
			if isnull {
				return fmt.Errorf("option %q not found", opt.Name)
			}
			err := updateSource(dc, node.DataSourceName, opt.Name, "NULL")
			if err != nil {
				return fmt.Errorf("unable to drop option %q", opt.Name)
			}
		}
		if opt.Action == "SET" {
			if isnull {
				return fmt.Errorf("option %q not found", opt.Name)
			}
			err := updateSource(dc, node.DataSourceName, opt.Name, "'"+opt.Val+"'")
			if err != nil {
				return fmt.Errorf("unable to set option %q", opt.Name)
			}
		}
		if opt.Action == "ADD" {
			if !isnull {
				return fmt.Errorf("option %q provided more than once", opt.Name)
			}
			err := updateSource(dc, node.DataSourceName, opt.Name, "'"+opt.Val+"'")
			if err != nil {
				return fmt.Errorf("unable to add option %q", opt.Name)
			}
		}
	}

	return nil
}

func isSourceOptionNull(dc *pgx.Conn, sourceName, optionName string) (bool, error) {
	var val *string
	q := "SELECT " + optionName + " FROM metadb.source WHERE name='" + sourceName + "'"
	err := dc.QueryRow(context.TODO(), q).Scan(&val)
	switch {
	case err == pgx.ErrNoRows:
		return false, fmt.Errorf("data source %q does not exist", sourceName)
	case err != nil:
		return false, fmt.Errorf("reading data source: %v", err)
	default:
		return val == nil, nil
	}
}

func updateSource(dc *pgx.Conn, sourceName, optionName, valueText string) error {
	q := "UPDATE metadb.source SET " + optionName + "=" + valueText + " WHERE name='" + sourceName + "'"
	_, err := dc.Exec(context.TODO(), q)
	return err
}

func createSourceOptions(options []ast.Option) (*sysdb.SourceConnector, error) {
	err := checkOptionDuplicates(options)
	if err != nil {
		return nil, err
	}
	s := &sysdb.SourceConnector{
		// Set default values
		Enable:           true,
		Security:         "ssl",
		Topics:           []string{},
		SchemaPassFilter: []string{},
		SchemaStopFilter: []string{},
		TableStopFilter:  []string{},
	}
	for _, opt := range options {
		switch strings.ToLower(opt.Name) {
		case "brokers":
			s.Brokers = opt.Val
		case "security":
			s.Security = opt.Val
		case "topics":
			s.Topics = strings.Split(opt.Val, ",")
		case "consumergroup":
			s.Group = opt.Val
		case "schemapassfilter":
			s.SchemaPassFilter = strings.Split(opt.Val, ",")
		case "schemastopfilter":
			s.SchemaStopFilter = strings.Split(opt.Val, ",")
		case "tablestopfilter":
			s.TableStopFilter = strings.Split(opt.Val, ",")
		case "trimschemaprefix":
			s.TrimSchemaPrefix = opt.Val
		case "addschemaprefix":
			s.AddSchemaPrefix = opt.Val
		//case "enable":
		//	s.Enable = (strings.ToLower(opt.Val) == "true")
		case "module":
			s.Module = opt.Val
		default:
			return nil, &dberr.Error{
				Err: fmt.Errorf("invalid option %q", opt.Name),
				Hint: "Valid options in this context are: " +
					"brokers, security, topics, consumergroup, schemapassfilter, schemastopfilter, tablestopfilter, trimschemaprefix, addschemaprefix, module",
			}
		}
	}
	return s, nil
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

func createUser(conn net.Conn /*query string,*/, node *ast.CreateUserStmt, db *dbx.DB, dc *pgx.Conn) error {
	if node.Options == nil {
		// return to client
	}
	opt, err := createUserOptions(node.Options)
	if err != nil {
		return err
	}
	if opt.Password == "" {
		return fmt.Errorf("option \"password\" is required")
	}

	dcsuper, err := db.ConnectSuper()
	if err != nil {
		return err
	}
	defer dbx.Close(dcsuper)

	exists, err := userExists(dcsuper, node.UserName)
	if err != nil {
		return fmt.Errorf("selecting role: %v", err)
	}
	if exists {
		_ = write(conn, encode(nil, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "NOTICE",
			Message: fmt.Sprintf("role %q already exists, skipping", node.UserName)},
		}))
	} else {
		q := "CREATE USER " + node.UserName + " PASSWORD '" + opt.Password + "'"
		if _, err = dcsuper.Exec(context.TODO(), q); err != nil {
			return err
		}

	}

	// Add comment on role.
	if opt.Comment != "" {
		q := "COMMENT ON ROLE " + node.UserName + " IS '" + opt.Comment + "'"
		if _, err = dcsuper.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("adding comment on role %s: %s", node.UserName, err)
		}
	}

	q := "CREATE SCHEMA IF NOT EXISTS " + node.UserName
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating schema %s: %s", node.UserName, err)
	}
	q = "GRANT CREATE ON SCHEMA " + node.UserName + " TO " + node.UserName
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("granting create privilege on schema %q to role %q: %s", node.UserName, node.UserName, err)
	}
	q = "GRANT USAGE ON SCHEMA " + node.UserName + " TO " + node.UserName + " WITH GRANT OPTION"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("granting usage privilege on schema %q to role %q: %s", node.UserName, node.UserName, err)
	}

	b := encode(nil, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("CREATE ROLE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
}

func createUserOptions(options []ast.Option) (*userOptions, error) {
	err := checkOptionDuplicates(options)
	if err != nil {
		return nil, err
	}
	o := &userOptions{
		// Set default values
	}
	for _, opt := range options {
		switch strings.ToLower(opt.Name) {
		case "password":
			o.Password = opt.Val
		case "comment":
			o.Comment = opt.Val
		default:
			return nil, &dberr.Error{
				Err:  fmt.Errorf("invalid option %q", opt.Name),
				Hint: "Valid options in this context are: password, comment",
			}
		}
	}
	return o, nil
}

type userOptions struct {
	Password string
	Comment  string
}

func authorize(conn net.Conn, node *ast.AuthorizeStmt, dc *pgx.Conn) error {
	exists, err := sourceExists(dc, node.DataSourceName)
	if err != nil {
		return fmt.Errorf("selecting data source: %v", err)
	}
	if !exists {
		return fmt.Errorf("data source %q does not exist", node.DataSourceName)
	}

	exists, err = userExists(dc, node.RoleName)
	if err != nil {
		return fmt.Errorf("selecting role: %v", err)
	}
	if !exists {
		return fmt.Errorf("role %q does not exist", node.RoleName)
	}

	q := "INSERT INTO metadb.auth(username,tables,dbupdated) VALUES ('" + node.RoleName + "','.*',FALSE) ON CONFLICT (username) DO UPDATE SET tables='.*',dbupdated=FALSE;"
	_, err = dc.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("writing authorization: %v", err)
	}

	_ = write(conn, encode(nil, []pgproto3.Message{
		&pgproto3.NoticeResponse{Severity: "INFO", Message: "restart server to update all permissions"},
	}))

	b := encode(nil, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("AUTHORIZE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
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

// TODO move to catalog package
func userExists(dc *pgx.Conn, username string) (bool, error) {
	q := "SELECT 1 FROM pg_catalog.pg_user WHERE usename=$1"
	var i int64
	err := dc.QueryRow(context.TODO(), q, username).Scan(&i)
	switch {
	case err == pgx.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func createDataOrigin(conn net.Conn, node *ast.CreateDataOriginStmt, dc *pgx.Conn) error {
	if len(node.OriginName) > 63 {
		return fmt.Errorf("data origin name %q too long", node.OriginName)
	}

	exists, err := originExists(dc, node.OriginName)
	if err != nil {
		return fmt.Errorf("selecting data origin: %v", err)
	}
	if exists {
		return fmt.Errorf("data origin %q already exists", node.OriginName)
	}

	q := "INSERT INTO metadb.origin(name)VALUES($1)"
	_, err = dc.Exec(context.TODO(), q, node.OriginName)
	if err != nil {
		return fmt.Errorf("writing origin configuration: %v", err)
	}

	_ = write(conn, encode(nil, []pgproto3.Message{
		&pgproto3.NoticeResponse{Severity: "INFO", Message: "restart server for new origin to take effect"},
	}))

	b := encode(nil, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("CREATE DATA ORIGIN")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
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

func refreshInferredColumnTypesStmt(conn net.Conn, dc *pgx.Conn) error {
	err := tools.RefreshInferredColumnTypes(dc, func(msg string) {
		_ = write(conn, encode(nil, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "INFO",
			Message: msg},
		}))
	})
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	b := encode(nil, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("REFRESH INFERRED COLUMN TYPES")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
}

func verifyConsistencyStmt(conn net.Conn, dc *pgx.Conn) error {
	err := tools.VerifyConsistency(dc, func(msg string) {
		_ = write(conn, encode(nil, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "INFO",
			Message: msg},
		}))
	})
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	b := encode(nil, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("VERIFY CONSISTENCY")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
	return write(conn, b)
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
