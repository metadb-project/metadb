package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/dsync"
	"github.com/metadb-project/metadb/cmd/metadb/libpq"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/marctab"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/runsql"
	"github.com/metadb-project/metadb/cmd/metadb/sqlfunc"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

// Notifier interface
type Notifier interface {
	Notify(ctx context.Context, message string) error
}

// The server thread handling needs to be reworked.  It currently runs an HTTP
// server and a single poll loop in two goroutines.

type server struct {
	opt   *option.Server
	state serverstate
	db    *dbx.DB
	//dc      *pgx.Conn
	//dcsuper *pgx.Conn
	dp       *pgxpool.Pool
	notifier Notifier
}

// serverstate is shared between goroutines.
type serverstate struct {
	mu        sync.Mutex
	databases []*sysdb.DatabaseConnector
	sources   []*sysdb.SourceConnector
}

// sproc stores state for a single poll loop.
type sproc struct {
	schemaPassFilter []*regexp.Regexp
	schemaStopFilter []*regexp.Regexp
	tableStopFilter  []*regexp.Regexp
	source           *sysdb.SourceConnector
	databases        []*sysdb.DatabaseConnector
	sourceLog        *log.SourceLog
	svr              *server
}

func Start(opt *option.Server, ntf Notifier) error {
	// Check if server is already running.
	running, pid, err := process.IsServerRunning(opt.Datadir)
	if err != nil {
		return err
	}
	if running {
		log.Fatal("lock file %q already exists and server (PID %d) appears to be running", util.SystemPIDFileName(opt.Datadir), pid)
		return fmt.Errorf("could not start server")
	}
	// Write lock file for new server instance.
	if err = process.WritePIDFile(opt.Datadir); err != nil {
		return err
	}
	defer process.RemovePIDFile(opt.Datadir)

	var svr = &server{
		opt:      opt,
		notifier: ntf,
	}

	if err = loggingServer(svr); err != nil {
		return err
	}
	return nil
}

func loggingServer(svr *server) error {
	// Read database URL from config file.
	var err error
	svr.db, err = util.ReadConfigDatabase(svr.opt.Datadir)
	if err != nil {
		return fmt.Errorf("reading configuration file: %v", err)
	}

	svr.dp, err = dbx.NewPool(context.TODO(), svr.db.ConnString(svr.db.User, svr.db.Password))
	if err != nil {
		return fmt.Errorf("creating database connection pool: %v", err)
	}
	defer svr.dp.Close()

	// Check that database is initialized and compatible
	cat, err := catalog.Initialize(svr.db, svr.dp)
	if err != nil {
		return err
	}

	log.SetDatabase(svr.dp)
	defer log.SetDatabase(nil)

	log.Info("starting Metadb %s", util.MetadbVersion)
	if err := runServer(svr, cat); err != nil {
		log.Fatal("%s", err)
		return err
	}
	return nil
}

func runServer(svr *server, cat *catalog.Catalog) error {
	if svr.db.DBName != "metadb" && !strings.HasPrefix(svr.db.DBName, "metadb_") {
		log.Info("database has nonstandard name %q", svr.db.DBName)
	}
	setMemoryLimit(svr.opt.MemoryLimit)
	if svr.opt.NoTLS {
		log.Warning("TLS disabled for all client connections")
	}
	if err := launchServer(svr, cat); err != nil {
		return err
	}
	//log.Info("server is shut down")
	return nil
}

func setMemoryLimit(limit float64) {
	// limit is specified in GiB.
	debug.SetMemoryLimit(int64(math.Min(math.Max(0.122, limit), 16.0) * 1073741824))
}

func launchServer(svr *server, cat *catalog.Catalog) error {
	return mainServer(svr, cat)
}

func mainServer(svr *server, cat *catalog.Catalog) error {
	var sigc = make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM)
	go func() {
		<-sigc
		log.Debug("received shutdown request")
		log.Info("shutting down")
		process.SetStop()
	}()
	// TODO also need to catch signals and call RemovePIDFile

	//svr.dc, err = svr.db.Connect()
	//if err != nil {
	//	return err
	//}
	//defer dbx.Close(svr.dc)
	//
	//svr.dcsuper, err = svr.db.ConnectSuper()
	//if err != nil {
	//	return err
	//}
	//defer dbx.Close(svr.dcsuper)

	// Check that database version is compatible
	// err = sysdb.ValidateSysdbVersion(svr.db)
	// if err != nil {
	// 	return err
	// }

	// if svr.conn, err = pgxpool.Connect(context.TODO(), svr.connString); err != nil {
	// 	return err
	// }

	// spr, err := waitForConfig(svr)
	// if err != nil {
	// 	log.Fatal("%s", err)
	// 	os.Exit(1)
	// }

	//go listenAndServe(svr)

	go goCreateFunctions(*(svr.db))

	go libpq.Listen(svr.opt.Listen, svr.opt.Port, svr.db, &svr.state.sources)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go goPollLoop(ctx, cat, svr)

	/*	folio, err := isFolioModulePresent(svr.db)
		if err != nil {
			return fmt.Errorf("checking for folio module: %v", err)
		}
		reshare, err := isReshareModulePresent(svr.db)
		if err != nil {
			return fmt.Errorf("checking for reshare module: %v", err)
		}
		source, err := util.GetOneSource(svr.dp)
		if err != nil {
			log.Info("reading source: %v", err)
		}
		go goMaintenance(svr.opt.Datadir, *(svr.db), cat, source, folio, reshare)
	*/

	for {
		if process.Stop() {
			break
		}
		time.Sleep(5 * time.Second)
	}

	return nil
}

func isFolioModulePresent(db *dbx.DB) (bool, error) {
	dc, err := db.Connect()
	if err != nil {
		return false, fmt.Errorf("connecting to database: %v", err)
	}
	defer dbx.Close(dc)
	q := "SELECT 1 FROM metadb.source WHERE module='folio' LIMIT 1"
	var n int32
	err = dc.QueryRow(context.TODO(), q).Scan(&n)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("selecting module: %v", err)
	default:
		return true, nil
	}
}

func isReshareModulePresent(db *dbx.DB) (bool, error) {
	dc, err := db.Connect()
	if err != nil {
		return false, fmt.Errorf("connecting to database: %v", err)
	}
	defer dbx.Close(dc)
	q := "SELECT 1 FROM metadb.source WHERE module='reshare' LIMIT 1"
	var n int32
	err = dc.QueryRow(context.TODO(), q).Scan(&n)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("selecting module: %v", err)
	default:
		return true, nil
	}
}

func goMaintenance(datadir string, db dbx.DB, dp *pgxpool.Pool, cat *catalog.Catalog, source string, folio, reshare bool) {
	for {
		//stat := dp.Stat()
		//log.Info("connection statistics: AcquireCount=%v AcquireDuration=%v AcquiredConns=%v CanceledAcquireCount=%v ConstructingConns=%v EmptyAcquireCount=%v IdleConns=%v MaxConns=%v MaxIdleDestroyCount=%v MaxLifetimeDestroyCount=%v NewConnsCount=%v TotalConns=%v",
		//	stat.AcquireCount(), stat.AcquireDuration(), stat.AcquiredConns(), stat.CanceledAcquireCount(), stat.ConstructingConns(), stat.EmptyAcquireCount(), stat.IdleConns(), stat.MaxConns(), stat.MaxIdleDestroyCount(), stat.MaxLifetimeDestroyCount(), stat.NewConnsCount(), stat.TotalConns())
		time.Sleep(5 * time.Minute)
		syncMode, err := dsync.ReadSyncMode(dp, source)
		if err != nil {
			log.Error("unable to read sync mode: %v", err)
		}
		if folio && syncMode == dsync.NoSync {
			if err := marctab.RunMarctab(db, datadir, cat); err != nil {
				log.Error("marc__t: %v", err)
			}
		}
		if err := checkTimeDailyMaintenance(datadir, db, dp, cat, source, folio, reshare, syncMode); err != nil {
			log.Error("%v", err)
		}
		time.Sleep(55 * time.Minute)
	}
}

func checkTimeDailyMaintenance(datadir string, db dbx.DB, dp *pgxpool.Pool, cat *catalog.Catalog, source string, folio, reshare bool, syncMode dsync.Mode) error {
	var overdue bool
	q := "SELECT CURRENT_TIMESTAMP > next_maintenance_time FROM metadb.maintenance"
	err := dp.QueryRow(context.TODO(), q).Scan(&overdue)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		fallthrough
	case err != nil:
		return fmt.Errorf("checking maintenance time: %v", err)
	default:
		if !overdue {
			return nil
		}
	}

	log.Debug("starting maintenance")

	if folio && syncMode == dsync.NoSync {
		tries := 0
		for {
			tries++
			url := "https://github.com/folio-org/folio-analytics.git"
			ref := "refs/tags/v1.7.8"
			path := "sql_metadb/derived_tables"
			schema := "folio_derived"
			if err = runsql.RunSQL(datadir, cat, db, url, ref, path, schema, source); err != nil {
				log.Warning("runsql: %v: repository=%s ref=%s path=%s", err, url, ref, path)
				if tries >= 12 {
					break
				}
				time.Sleep(1 * time.Hour)
				continue
			}
			break
		}
	}
	reshareRef := "refs/tags/20230912004531"
	if reshare && syncMode == dsync.NoSync {
		tries := 0
		for {
			tries++
			url := "https://github.com/openlibraryenvironment/reshare-analytics.git"
			ref := reshareRef
			path := "reports"
			schema := "report"
			if err = sqlfunc.SQLFunc(datadir, cat, db, url, ref, path, schema, source); err != nil {
				log.Warning("sqlfunc: %v: repository=%s ref=%s path=%s", err, url, ref, path)
				if tries >= 12 {
					break
				}
				time.Sleep(1 * time.Hour)
				continue
			}
			break
		}
	}
	if reshare && syncMode == dsync.NoSync {
		tries := 0
		for {
			tries++
			url := "https://github.com/openlibraryenvironment/reshare-analytics.git"
			ref := reshareRef
			path := "sql/derived_tables"
			schema := "reshare_derived"
			if err = runsql.RunSQL(datadir, cat, db, url, ref, path, schema, source); err != nil {
				log.Warning("runsql: %v: repository=%s ref=%s path=%s", err, url, ref, path)
				if tries >= 12 {
					break
				}
				time.Sleep(1 * time.Hour)
				continue
			}
			break
		}
	}

	// Schedule next maintenance
	q = "UPDATE metadb.maintenance " +
		"SET next_maintenance_time = next_maintenance_time +" +
		" make_interval(0, 0, 0, (EXTRACT(DAY FROM (CURRENT_TIMESTAMP - next_maintenance_time)) + 1)::integer)"
	if _, err = dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("error updating maintenance time: %v", err)
	}

	// if err = vacuumAll(db, cat, folio); err != nil {
	// 	return err
	// }

	log.Debug("completed maintenance")
	return nil
}

/*
func vacuumAll(db dbx.DB, cat *catalog.Catalog, folio bool) error {
	dcsuper, err := db.ConnectSuper()
	if err != nil {
		return err
	}
	defer dbx.Close(dcsuper)

	for _, t := range catalog.SystemTables() {
		log.Trace("vacuuming table %s", t)
		if err = dbx.VacuumAnalyze(dcsuper, t); err != nil {
			return err
		}
	}
	for _, t := range cat.AllTables() {
		m := t.Main()
		log.Trace("vacuuming table %s", m)
		if err = dbx.VacuumAnalyze(dcsuper, m); err != nil {
			return err
		}
	}
	if folio {
		for _, t := range []dbx.Table{{Schema: "marctab", Table: "cksum"}, {Schema: "folio_source_record", Table: "marc__t"}} {
			log.Trace("vacuuming table %s", t)
			_ = dbx.VacuumAnalyze(dcsuper, t)
		}
	}
	return nil
}
*/

func goCreateFunctions(db dbx.DB) {
	dc, err := db.Connect()
	if err != nil {
		log.Error("%v", err)
		return
	}
	defer dbx.Close(dc)

	dcsuper, err := db.ConnectSuper()
	if err != nil {
		log.Error("%v", err)
		return
	}
	defer dbx.Close(dcsuper)

	err = catalog.CreateAllFunctions(dcsuper, dc, db.User)
	if err != nil {
		log.Error("updating functions: %v", err)
		return
	}
}

//func goListenAndServe(svr *server) {
//        var err error
//        if err = listenAndServe(svr); err != nil {
//                eout.Error("%s", err)
//        }
//}

/*func listenAndServe(svr *server) {
	var err error
	var host string
	if svr.opt.Listen == "" {
		host = "127.0.0.1"
	} else {
		host = svr.opt.Listen
	}
	// var port = svr.opt.Port
	port := "8441"
	var httpsvr = http.Server{
		Addr:    net.JoinHostPort(host, port),
		Handler: setupHandlers(svr),
	}
	//log.Info("listening on address \"%s\", port %s", host, port)
	//log.Info("server is ready to accept connections")
	if svr.opt.Listen == "" || svr.opt.NoTLS {
		if err = httpsvr.ListenAndServe(); err != nil {
			// TODO error handling
			//return fmt.Errorf("error starting server: %s", err)
			_ = err
		}
	} else {
		if err = httpsvr.ListenAndServeTLS(svr.opt.TLSCert, svr.opt.TLSKey); err != nil {
			// TODO error handling
			//return fmt.Errorf("error starting server: %s", err)
			_ = err
		}
	}
}
*/
/*func unsupportedMethod(path string, r *http.Request) string {
	return fmt.Sprintf("%s: unsupported method: %s", path, r.Method)
}
*/
/*func errorWritingResponse(r *http.Request) string {
	return fmt.Sprintf("%s: error writing HTTP response", r.Method)
}
*/
/*func requestString(r *http.Request) string {
	var remoteHost, remotePort string
	remoteHost, remotePort, _ = net.SplitHostPort(r.RemoteAddr)
	return fmt.Sprintf("host=%s port=%s method=%s uri=%s", remoteHost, remotePort, r.Method, r.URL)
}
*/
/*func setupHandlers(svr *server) http.Handler {

	mux := http.NewServeMux()

	mux.HandleFunc("/config", svr.handleConfig)
	mux.HandleFunc("/enable", svr.handleEnable)
	mux.HandleFunc("/disable", svr.handleDisable)
	mux.HandleFunc("/status", svr.handleStatus)
	mux.HandleFunc("/user", svr.handleUser)
	mux.HandleFunc("/", svr.handleDefault)

	return mux
}
*/
/*func (svr *server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		log.Debug("request: %s", requestString(r))
		svr.handleConfigGet(w, r)
		return
	}
	if r.Method == "POST" {
		log.Debug("request: %s", requestString(r))
		svr.handleConfigPost(w, r)
		return
	}
	if r.Method == "DELETE" {
		log.Debug("request: %s", requestString(r))
		svr.handleConfigDelete(w, r)
		return
	}
	var m = unsupportedMethod("/config", r)
	log.Info(m)
	http.Error(w, m, http.StatusMethodNotAllowed)
}
*/
/*func (svr *server) handleUser(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		log.Debug("request: %s", requestString(r))
		svr.handleUserGet(w, r)
		return
	}
	if r.Method == "POST" {
		log.Debug("request: %s", requestString(r))
		svr.handleUserPost(w, r)
		return
	}
	if r.Method == "DELETE" {
		log.Debug("request: %s", requestString(r))
		svr.handleUserDelete(w, r)
		return
	}
	var m = unsupportedMethod("/user", r)
	log.Info(m)
	http.Error(w, m, http.StatusMethodNotAllowed)
}
*/
/*func (svr *server) handleEnable(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		log.Debug("request: %s", requestString(r))
		w.Header().Set("Content-Type", "text/plain")
		_, err := fmt.Fprintf(w, "enable\r\n")
		if err != nil {
			http.Error(w, errorWritingResponse(r), http.StatusInternalServerError)
			return
		}
		return
	}
	if r.Method == "POST" {
		log.Debug("request: %s", requestString(r))
		svr.handleEnablePost(w, r)
		return
	}
	var m = unsupportedMethod("/enable", r)
	log.Info(m)
	http.Error(w, m, http.StatusMethodNotAllowed)
}
*/
/*func (svr *server) handleDisable(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		log.Debug("request: %s", requestString(r))
		w.Header().Set("Content-Type", "text/plain")
		_, err := fmt.Fprintf(w, "disable\r\n")
		if err != nil {
			http.Error(w, errorWritingResponse(r), http.StatusInternalServerError)
			return
		}
		return
	}
	if r.Method == "POST" {
		log.Debug("request: %s", requestString(r))
		svr.handleDisablePost(w, r)
		return
	}
	var m = unsupportedMethod("/disable", r)
	log.Info(m)
	http.Error(w, m, http.StatusMethodNotAllowed)
}
*/
/*func (svr *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		log.Debug("request: %s", requestString(r))
		svr.handleStatusGet(w, r)
		return
	}
	var m = unsupportedMethod("/status", r)
	log.Info(m)
	http.Error(w, m, http.StatusMethodNotAllowed)
}
*/
/*func (svr *server) handleConfigGet(w http.ResponseWriter, r *http.Request) {
	// read request
	var rq api.ConfigListRequest
	var ok bool
	if ok = util.ReadRequest(w, r, &rq); !ok {
		return
	}
	// retrieve config
	var rs *api.ConfigListResponse
	var err error
	if rs, err = sysdb.ListConfig(&rq); err != nil {
		util.HandleError(w, err, http.StatusInternalServerError)
		return
	}
	// success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err = json.NewEncoder(w).Encode(rs); err != nil {
		util.HandleError(w, err, http.StatusInternalServerError)
	}
}
*/
/*func (svr *server) handleUserGet(w http.ResponseWriter, r *http.Request) {
		// read request
		var rq api.UserListRequest
		var ok bool
		if ok = util.ReadRequest(w, r, &rq); !ok {
			return
		}
		// retrieve user
		var rs *api.UserListResponse
		var err error
		if rs, err = sysdb.ListUser(&rq); err != nil {
			util.HandleError(w, err, http.StatusInternalServerError)
			return
		}
		// success response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err = json.NewEncoder(w).Encode(rs); err != nil {
			util.HandleError(w, err, http.StatusInternalServerError)
		}
}
*/
/*func (svr *server) handleStatusGet(w http.ResponseWriter, r *http.Request) {
	svr.state.mu.Lock()
	defer svr.state.mu.Unlock()

	var p api.GetStatusRequest
	var ok bool
	if ok = util.ReadRequest(w, r, &p); !ok {
		return
	}

	var stat api.GetStatusResponse
	stat.Databases = make(map[string]status.Status)
	stat.Sources = make(map[string]status.Status)

	var d *sysdb.DatabaseConnector
	for _, d = range svr.state.databases {
		stat.Databases["database"] = d.Status
	}

	var s *sysdb.SourceConnector
	for _, s = range svr.state.sources {
		stat.Sources[s.Name] = s.Status
	}

	// Respond with success.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	var err error
	if err = json.NewEncoder(w).Encode(stat); err != nil {
		// TODO error handling
		_ = err
	}
}
*/
/*func (svr *server) handleConfigDelete(w http.ResponseWriter, r *http.Request) {
	// read request
	var rq api.ConfigDeleteRequest
	var ok bool
	if ok = util.ReadRequest(w, r, &rq); !ok {
		return
	}
	// delete config
	var rs *api.ConfigDeleteResponse
	var err error
	if rs, err = sysdb.DeleteConfig(&rq); err != nil {
		util.HandleError(w, err, http.StatusInternalServerError)
		return
	}
	// success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err = json.NewEncoder(w).Encode(rs); err != nil {
		util.HandleError(w, err, http.StatusInternalServerError)
	}
}
*/
/*func (svr *server) handleUserDelete(w http.ResponseWriter, r *http.Request) {
	// read request
	var rq api.UserDeleteRequest
	var ok bool
	if ok = util.ReadRequest(w, r, &rq); !ok {
		return
	}
	// delete user
	var rs *api.UserDeleteResponse
	var err error
	if rs, err = sysdb.DeleteUser(&rq); err != nil {
		util.HandleError(w, err, http.StatusInternalServerError)
		return
	}
	// success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err = json.NewEncoder(w).Encode(rs); err != nil {
		util.HandleError(w, err, http.StatusInternalServerError)
	}
}
*/
/*func (svr *server) handleConfigPost(w http.ResponseWriter, r *http.Request) {
	// read request
	var rq api.ConfigUpdateRequest
	var ok bool
	if ok = util.ReadRequest(w, r, &rq); !ok {
		return
	}
	// write config
	var err error
	if err = sysdb.UpdateConfig(&rq); err != nil {
		util.HandleError(w, err, http.StatusInternalServerError)
		return
	}
	// success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
}
*/
/*func (svr *server) handleUserPost(w http.ResponseWriter, r *http.Request) {
	// read request
	var rq api.UserUpdateRequest
	var ok bool
	if ok = util.ReadRequest(w, r, &rq); !ok {
		return
	}
	// Create user
	if rq.Create {
		if err := svr.createUser(&rq); err != nil {
			util.HandleError(w, err, http.StatusInternalServerError)
			return
		}
	}
	// write user
	if err := sysdb.UpdateUser(&rq); err != nil {
		util.HandleError(w, err, http.StatusInternalServerError)
		return
	}
	// success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
}
*/
/*func (svr *server) createUser(rq *api.UserUpdateRequest) error {
	svr.state.mu.Lock()
	defer svr.state.mu.Unlock()

	for _, dbc := range svr.state.databases {
		err := createUserInDB(rq, dbc)
		if err != nil {
			return err
		}
	}
	return nil
}
*/
/*func createUserInDB(rq *api.UserUpdateRequest, dbc *sysdb.DatabaseConnector) error {
	// dsn := &sqlx.DSN{
	// 	Host:     dbc.DBHost,
	// 	Port:     dbc.DBPort,
	// 	User:     dbc.DBSuperUser,
	// 	Password: dbc.DBSuperPassword,
	// 	DBName:   dbc.DBName,
	// 	SSLMode:  dbc.DBSSLMode,
	// 	Account:  dbc.DBAccount,
	// }
	dsn := &sqlx.DSN{
		// DBURI: "",
		Host:     dbc.DBHost,
		Port:     "5432",
		User:     dbc.DBSuperUser,
		Password: dbc.DBSuperPassword,
		DBName:   dbc.DBName,
		SSLMode:  "require",
		// Account:  dbc.DBAccount,
	}
	dbsuper, err := sqlx.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer dbsuper.Close()
	if err = dbsuper.Ping(); err != nil {
		return err
	}
	if err != nil {
		return fmt.Errorf("user: create: %s", err)
	}
	_, err = dbsuper.Exec(nil, "CREATE USER \""+rq.Name+"\" PASSWORD '"+rq.Password+"'")
	if err != nil {
		return fmt.Errorf("unable to create user %q: %s", rq.Name, err)
	}
	dsn = &sqlx.DSN{
		// DBURI: "",
		Host:     dbc.DBHost,
		Port:     "5432",
		User:     dbc.DBAdminUser,
		Password: dbc.DBAdminPassword,
		DBName:   dbc.DBName,
		SSLMode:  "require",
		// Account:  dbc.DBAccount,
	}
	db, err := sqlx.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		return err
	}
	if err != nil {
		return fmt.Errorf("user: create: %s", err)
	}
	_, err = db.Exec(nil, "CREATE SCHEMA \""+rq.Name+"\"")
	if err != nil {
		return fmt.Errorf("unable to create schema for user %q: %s", rq.Name, err)
	}
	_, err = db.Exec(nil, "GRANT CREATE, USAGE ON SCHEMA \""+rq.Name+"\" TO \""+rq.Name+"\"")
	if err != nil {
		log.Warning("unable to grant permissions on schema for user %q: %s", rq.Name, err)
	}
	return nil
}
*/
/*func (svr *server) handleEnablePost(w http.ResponseWriter, r *http.Request) {
	// read request
	var rq api.EnableRequest
	var ok bool
	if ok = util.ReadRequest(w, r, &rq); !ok {
		return
	}
	// enable
	var err error
	if err = sysdb.EnableConnector(&rq); err != nil {
		util.HandleError(w, err, http.StatusBadRequest)
		return
	}
	// success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
}
*/
/*func (svr *server) handleDisablePost(w http.ResponseWriter, r *http.Request) {
	// read request
	var rq api.DisableRequest
	var ok bool
	if ok = util.ReadRequest(w, r, &rq); !ok {
		return
	}
	// disable
	var err error
	if err = sysdb.DisableConnector(&rq); err != nil {
		util.HandleError(w, err, http.StatusBadRequest)
		return
	}
	// success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
}
*/
/*func (svr *server) handleDefault(w http.ResponseWriter, r *http.Request) {
	util.HandleError(w, fmt.Errorf("unknown request: %s", requestString(r)), http.StatusNotFound)
	//log.Error(fmt.Sprintf("unknown request: %s", requestString(r)))
	//http.Error(w, "404 page not found", http.StatusNotFound)
}
*/
