package libpq

import (
	"context"
	"fmt"
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
	//log.Trace("connected to database")
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
			if err = processQuery(conn, m, db, dbconn, sources); err != nil {
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
		fmt.Printf("%#v\n", m)
		// &pgproto3.StartupMessage{
		//ProtocolVersion:0x30000,
		//Parameters:map[string]string{"application_name":"psql",
		//    "client_encoding":"UTF8",
		//    "database":"nrn",
		//    "user":"nrn"}
		//   }
		if err = handleStartup(conn, m); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown message: %v", msg)
	}
}

func processQuery(conn net.Conn, query *pgproto3.Query, db *dbx.DB, dbconn *pgx.Conn, sources *[]*sysdb.SourceConnector) error {
	var e string
	node, err, pass := parser.Parse(query.String)
	if err != nil {
		e = err.Error()
	}
	log.Trace("query received: query=%q node=%#v err=%q pass=%v\n", query.String, node, e, pass)
	if pass {
		err = proxyQuery(conn, query, node, db, dbconn)
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
	case *ast.AlterDataSourceStmt:
		err = alterDataSource(conn, n, dbconn)
	case *ast.DropDataSourceStmt:
		err = dropDataSource(conn, n, dbconn)
	case *ast.AuthorizeStmt:
		err = authorize(conn, n, dbconn)
	case *ast.CreateDataOriginStmt:
		err = createDataOrigin(conn, n, dbconn)
	case *ast.ListStatusStmt:
		err = listStatus(conn, n, dbconn, sources)
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
		log.Error("%v: %s", err, query.String)
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
	return write(conn, encode(nil, []pgproto3.Message{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "14.3.0"},
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

func listStatus(conn net.Conn, node *ast.ListStatusStmt, dc *pgx.Conn, sources *[]*sysdb.SourceConnector) error {
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
		"(name,brokers,security,topics,consumergroup,schemapassfilter,trimschemaprefix,addschemaprefix,module,enable)" +
		"VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)"
	_, err = dc.Exec(context.TODO(), q,
		name, src.Brokers, src.Security, strings.Join(src.Topics, ","), src.Group,
		strings.Join(src.SchemaPassFilter, ","), src.TrimSchemaPrefix, src.AddSchemaPrefix, src.Module,
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
					"brokers, security, topics, consumergroup, schemapassfilter, trimschemaprefix, addschemaprefix, module",
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
	}
	for _, opt := range options {
		switch opt.Name {
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
					"brokers, security, topics, consumergroup, schemapassfilter, trimschemaprefix, addschemaprefix, module",
			}
		}
	}
	return s, nil
}

func checkOptionDuplicates(options []ast.Option) error {
	m := make(map[string]bool)
	for _, opt := range options {
		if m[opt.Name] {
			return fmt.Errorf("option %q provided more than once", opt.Name)
		}
		m[opt.Name] = true
	}
	return nil
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

// TODO move to cat package
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
