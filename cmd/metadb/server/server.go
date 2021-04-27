package server

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/internal/status"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
)

type server struct {
	opt       *option.Server
	databases []*sysdb.DatabaseConnector
	sources   []*sysdb.SourceConnector
}

type sproc struct {
	db               []*sql.DB
	schemaPassFilter []*regexp.Regexp
	source           *sysdb.SourceConnector
	databases        []*sysdb.DatabaseConnector
	sourceLog        *log.SourceLog
	svr              *server
}

func Start(opt *option.Server) error {
	var err error
	if err = process.WritePIDFile(opt.Datadir); err != nil {
		return err
	}

	var svr = &server{opt: opt}
	// TODO check that database version is up to date
	// (validateDatabaseLatestVersion)
	if err = runServer(svr); err != nil {
		return err
	}
	return nil
}

func runServer(svr *server) error {
	log.Info("starting Metadb %s", svr.opt.MetadbVersion)
	if svr.opt.NoTLS {
		log.Warning("TLS disabled for all client connections")
	}

	var sigc = make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM)
	go func() {
		_ = <-sigc
		log.Info("received shutdown request")
		log.Info("shutting down")
		process.SetStop()
	}()
	// TODO also need to catch signals and call RemovePIDFile

	go listenAndServe(svr)
	go goPollLoop(svr)

	for {
		if process.Stop() {
			break
		}
		time.Sleep(5 * time.Second)
	}

	process.RemovePIDFile(svr.opt.Datadir)

	return nil
}

func goListenAndServe(svr *server) {
	var err error
	if err = listenAndServe(svr); err != nil {
		eout.Error("%s", err)
	}
}

func listenAndServe(svr *server) error {
	var err error
	var host string
	if svr.opt.Listen == "" {
		host = "127.0.0.1"
	} else {
		host = svr.opt.Listen
	}
	var port = svr.opt.AdminPort
	var httpsvr = http.Server{
		Addr:    net.JoinHostPort(host, port),
		Handler: setupHandlers(svr),
	}
	log.Info("listening on address \"%s\", port %s", host, port)
	log.Info("server is ready to accept connections")
	if svr.opt.Listen == "" || svr.opt.NoTLS {
		if err = httpsvr.ListenAndServe(); err != nil {
			return fmt.Errorf("error starting server: %s", err)
		}
	} else {
		if err = httpsvr.ListenAndServeTLS(svr.opt.TLSCert, svr.opt.TLSKey); err != nil {
			return fmt.Errorf("error starting server: %s", err)
		}
	}
	return nil
}

func unsupportedMethod(path string, r *http.Request) string {
	return fmt.Sprintf("%s: unsupported method: %s", path, r.Method)
}

func requestString(r *http.Request) string {
	var remoteHost, remotePort string
	remoteHost, remotePort, _ = net.SplitHostPort(r.RemoteAddr)
	return fmt.Sprintf("host=%s port=%s method=%s uri=%s", remoteHost, remotePort, r.Method, r.URL)
}

func setupHandlers(svr *server) http.Handler {

	mux := http.NewServeMux()

	mux.HandleFunc("/databases", svr.handleDatabases)
	mux.HandleFunc("/sources", svr.handleSources)
	mux.HandleFunc("/status", svr.handleStatus)
	mux.HandleFunc("/", svr.handleDefault)

	return mux
}

func (svr *server) handleDatabases(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		log.Debug("request: %s", requestString(r))
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "databases\r\n")
		return
	}
	if r.Method == "POST" {
		log.Debug("request: %s", requestString(r))
		svr.handleDatabasesPost(w, r)
		return
	}
	var m = unsupportedMethod("/sources", r)
	log.Info(m)
	http.Error(w, m, http.StatusMethodNotAllowed)
}

func (svr *server) handleSources(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		log.Debug("request: %s", requestString(r))
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "sources\r\n")
		return
	}
	if r.Method == "POST" {
		log.Debug("request: %s", requestString(r))
		svr.handleSourcesPost(w, r)
		return
	}
	var m = unsupportedMethod("/sources", r)
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

func (svr *server) handleStatusGet(w http.ResponseWriter, r *http.Request) {

	// Authenticate user.
	var user string
	var ok bool
	user, ok = handleBasicAuth(w, r)
	if !ok {
		return
	}
	_ = user
	// Read the json request.
	var body []byte
	var err error
	body, err = ioutil.ReadAll(r.Body)
	if err != nil {
		handleError(w, err, http.StatusBadRequest)
		return
	}
	var p api.UpdateSourceConnectorRequest
	err = json.Unmarshal(body, &p)
	if err != nil {
		handleError(w, err, http.StatusBadRequest)
		return
	}
	log.Trace("request %s %v\n", r.RemoteAddr, p)

	// Write source data.

	var stat api.GetStatusResponse
	stat.Databases = make(map[string]status.Status)
	stat.Sources = make(map[string]status.Status)

	var d *sysdb.DatabaseConnector
	for _, d = range svr.databases {
		stat.Databases[d.Name] = d.Status
	}

	var s *sysdb.SourceConnector
	for _, s = range svr.sources {
		stat.Sources[s.Name] = s.Status
	}

	// Respond with success.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(stat)
}

func (svr *server) handleDatabasesPost(w http.ResponseWriter, r *http.Request) {

	// Authenticate user.
	var user string
	var ok bool
	user, ok = handleBasicAuth(w, r)
	if !ok {
		return
	}
	_ = user
	// Read the json request.
	var body []byte
	var err error
	body, err = ioutil.ReadAll(r.Body)
	if err != nil {
		handleError(w, err, http.StatusBadRequest)
		return
	}
	var p api.UpdateDatabaseConnectorRequest
	err = json.Unmarshal(body, &p)
	if err != nil {
		handleError(w, err, http.StatusBadRequest)
		return
	}
	log.Trace("request %s %v\n", r.RemoteAddr, p)

	// Write source data.

	if err = sysdb.UpdateDatabaseConnector(p); err != nil {
		handleError(w, err, http.StatusBadRequest)
		return
	}

	// Temporary - pause to allow for waitForConfig()
	time.Sleep(2 * time.Second)

	// Respond with success.
	w.WriteHeader(http.StatusCreated)
}

func (svr *server) handleSourcesPost(w http.ResponseWriter, r *http.Request) {

	// Authenticate user.
	var user string
	var ok bool
	user, ok = handleBasicAuth(w, r)
	if !ok {
		return
	}
	_ = user
	// Read the json request.
	var body []byte
	var err error
	body, err = ioutil.ReadAll(r.Body)
	if err != nil {
		handleError(w, err, http.StatusBadRequest)
		return
	}
	var p api.UpdateSourceConnectorRequest
	err = json.Unmarshal(body, &p)
	if err != nil {
		handleError(w, err, http.StatusBadRequest)
		return
	}
	log.Trace("request %s %v\n", r.RemoteAddr, p)

	// Write source data.

	if err = sysdb.UpdateKafkaSource(p); err != nil {
		handleError(w, err, http.StatusBadRequest)
		return
	}

	// Temporary - pause to allow for waitForConfig()
	time.Sleep(2 * time.Second)

	// Respond with success.
	w.WriteHeader(http.StatusCreated)
}

func handleBasicAuth(w http.ResponseWriter, r *http.Request) (
	string, bool) {
	var user, password string
	var ok bool
	user, password, ok = r.BasicAuth()
	if !ok {
		var m = "Unauthorized: Invalid HTTP Basic Authentication"
		log.Info(m)
		//w.Header().Set("WWW-Authenticate", "Basic")
		http.Error(w, m, http.StatusForbidden)
		return user, false
	}
	_ = password
	var match bool = true
	//var err error
	//match, err = srv.storage.Authenticate(user, password)
	//if err != nil {
	//        var m = "Unauthorized (user '" + user + "')"
	//        log.Println(m + ": " + err.Error())
	//        //w.Header().Set("WWW-Authenticate", "Basic")
	//        http.Error(w, m, http.StatusForbidden)
	//        return user, false
	//}
	if !match {
		var m = "Unauthorized (user '" + user + "'): " +
			"Unable to authenticate username/password"
		log.Info(m)
		//w.Header().Set("WWW-Authenticate", "Basic")
		http.Error(w, m, http.StatusForbidden)
		return user, false
	}
	return user, true
}

func (svr *server) handleDefault(w http.ResponseWriter, r *http.Request) {
	log.Error(fmt.Sprintf("unknown request: %s", requestString(r)))
	http.Error(w, "404 page not found", http.StatusNotFound)
}

func handleError(w http.ResponseWriter, err error, statusCode int) {
	log.Error("%s", err)
	HTTPError(w, err, statusCode)
}

func HTTPError(w http.ResponseWriter, err error, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	var m = map[string]interface{}{
		"status": "error",
		//"message": fmt.Sprintf("%s: %s", http.StatusText(code), err),
		"message": err.Error(),
		"code":    code,
		//"data":    "",
	}
	//json.NewEncoder(w).Encode(err)
	json.NewEncoder(w).Encode(m)
}
