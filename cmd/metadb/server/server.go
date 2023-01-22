package server

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/internal/status"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/libpq"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

// The server thread handling needs to be reworked.  It currently runs an HTTP
// server and a single poll loop in two goroutines.

type server struct {
	opt   *option.Server
	state serverstate
	db    *dbx.DB
	//dc      *pgx.Conn
	//dcsuper *pgx.Conn
}

// serverstate is shared between goroutines.
type serverstate struct {
	mu        sync.Mutex
	databases []*sysdb.DatabaseConnector
	sources   []*sysdb.SourceConnector
}

// sproc stores state for a single poll loop.
type sproc struct {
	db               []sqlx.DB
	schemaPassFilter []*regexp.Regexp
	schemaStopFilter []*regexp.Regexp
	source           *sysdb.SourceConnector
	databases        []*sysdb.DatabaseConnector
	sourceLog        *log.SourceLog
	svr              *server
}

func Start(opt *option.Server) error {
	// check if server is already running
	running, pid, err := isServerRunning(opt.Datadir)
	if err != nil {
		return err
	}
	if running {
		log.Fatal("lock file %q already exists and server (PID %d) appears to be running", util.SystemPIDFileName(opt.Datadir), pid)
		return fmt.Errorf("could not start server")
	}
	// write lock file for new server instance
	if err = process.WritePIDFile(opt.Datadir); err != nil {
		return err
	}

	var svr = &server{opt: opt}
	if err = runServer(svr); err != nil {
		return err
	}
	process.RemovePIDFile(svr.opt.Datadir)
	return nil
}

func isServerRunning(datadir string) (bool, int, error) {
	// check for lock file
	lockfile := util.SystemPIDFileName(datadir)
	fexists, err := util.FileExists(lockfile)
	if err != nil {
		return false, 0, fmt.Errorf("reading lock file %q: %s", lockfile, err)
	}
	if !fexists {
		return false, 0, nil
	}
	// read pid
	pid, err := process.ReadPIDFile(datadir)
	if err != nil {
		return false, 0, fmt.Errorf("reading lock file %q: %s", lockfile, err)
	}
	// check for running process
	p, err := os.FindProcess(pid)
	if err != nil {
		return false, 0, nil
	}
	err = p.Signal(syscall.Signal(0))
	if err != nil {
		errno, ok := err.(syscall.Errno)
		if !ok {
			return false, 0, nil
		}
		if errno == syscall.EPERM {
			return true, pid, nil
		} else {
			return false, 0, nil
		}
	}
	return true, pid, nil
}

func runServer(svr *server) error {
	log.Info("starting Metadb %s", util.MetadbVersion)
	// if svr.opt.RewriteJSON {
	// 	log.Info("enabled JSON rewriting")
	// }
	setMemoryLimit(svr.opt.MemoryLimit)

	if svr.opt.NoTLS {
		log.Warning("TLS disabled for all client connections")
	}
	return launchServer(svr)
}

func setMemoryLimit(limit float64) {
	// limit is specified in GiB.
	debug.SetMemoryLimit(int64(math.Min(math.Max(0.122, limit), 16.0) * 1073741824))
}

func launchServer(svr *server) error {
	return mainServer(svr)
}

func mainServer(svr *server) error {
	var sigc = make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM)
	go func() {
		<-sigc
		log.Info("received shutdown request")
		log.Info("shutting down")
		process.SetStop()
	}()
	// TODO also need to catch signals and call RemovePIDFile

	// Read database URL from config file.
	var err error
	svr.db, err = util.ReadConfigDatabase(svr.opt.Datadir)
	if err != nil {
		return fmt.Errorf("reading configuration file: %v", err)
	}

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

	// Check that database is initialized and compatible
	cat, err := catalog.Initialize(svr.db)
	if err != nil {
		return err
	}

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

	go goPollLoop(cat, svr)

	for {
		if process.Stop() {
			break
		}
		time.Sleep(5 * time.Second)
	}

	cat.Close()

	return nil
}

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
		log.Error("%v", err)
		return
	}
}

//func goListenAndServe(svr *server) {
//        var err error
//        if err = listenAndServe(svr); err != nil {
//                eout.Error("%s", err)
//        }
//}

func listenAndServe(svr *server) {
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
	log.Info("listening on address \"%s\", port %s", host, port)
	log.Info("server is ready to accept connections")
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

func unsupportedMethod(path string, r *http.Request) string {
	return fmt.Sprintf("%s: unsupported method: %s", path, r.Method)
}

func errorWritingResponse(r *http.Request) string {
	return fmt.Sprintf("%s: error writing HTTP response", r.Method)
}

func requestString(r *http.Request) string {
	var remoteHost, remotePort string
	remoteHost, remotePort, _ = net.SplitHostPort(r.RemoteAddr)
	return fmt.Sprintf("host=%s port=%s method=%s uri=%s", remoteHost, remotePort, r.Method, r.URL)
}

func setupHandlers(svr *server) http.Handler {

	mux := http.NewServeMux()

	mux.HandleFunc("/config", svr.handleConfig)
	mux.HandleFunc("/enable", svr.handleEnable)
	mux.HandleFunc("/disable", svr.handleDisable)
	mux.HandleFunc("/status", svr.handleStatus)
	mux.HandleFunc("/user", svr.handleUser)
	mux.HandleFunc("/", svr.handleDefault)

	return mux
}

func (svr *server) handleConfig(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleUser(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleEnable(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleDisable(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		log.Debug("request: %s", requestString(r))
		svr.handleStatusGet(w, r)
		return
	}
	var m = unsupportedMethod("/status", r)
	log.Info(m)
	http.Error(w, m, http.StatusMethodNotAllowed)
}

func (svr *server) handleConfigGet(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleUserGet(w http.ResponseWriter, r *http.Request) {
	/*	// read request
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
	*/
}

func (svr *server) handleStatusGet(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleConfigDelete(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleUserDelete(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleConfigPost(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleUserPost(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) createUser(rq *api.UserUpdateRequest) error {
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

func createUserInDB(rq *api.UserUpdateRequest, dbc *sysdb.DatabaseConnector) error {
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

func (svr *server) handleEnablePost(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleDisablePost(w http.ResponseWriter, r *http.Request) {
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

func (svr *server) handleDefault(w http.ResponseWriter, r *http.Request) {
	util.HandleError(w, fmt.Errorf("unknown request: %s", requestString(r)), http.StatusNotFound)
	//log.Error(fmt.Sprintf("unknown request: %s", requestString(r)))
	//http.Error(w, "404 page not found", http.StatusNotFound)
}
