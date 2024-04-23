package server

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/internal/status"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/change"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/dsync"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
	"golang.org/x/net/context"
)

func goPollLoop(ctx context.Context, cat *catalog.Catalog, svr *server) {
	if svr.opt.NoKafkaCommit {
		log.Info("Kafka commits disabled")
	}
	// For now, we support only one source
	spr, err := waitForConfig(svr)
	if err != nil {
		log.Fatal("%s", err)
		os.Exit(1)
	}

	err = logSyncMode(svr.dp, spr.source.Name)
	if err != nil {
		log.Fatal("%s", err)
		os.Exit(1)
	}

	folio, err := isFolioModulePresent(svr.db)
	if err != nil {
		log.Error("checking for folio module: %v", err)
	}
	reshare, err := isReshareModulePresent(svr.db)
	if err != nil {
		log.Error("checking for reshare module: %v", err)
	}
	go goMaintenance(svr.opt.Datadir, *(svr.db), svr.dp, cat, spr.source.Name, folio, reshare)

	for {
		err := launchPollLoop(ctx, cat, svr, spr)
		if err == nil {
			break
		}
		spr.source.Status.Error()
		spr.databases[0].Status.Error()
		time.Sleep(24 * time.Hour)
	}
}

func logSyncMode(dq dbx.Queryable, source string) error {
	mode, err := dsync.ReadSyncMode(dq, source)
	if err != nil {
		return fmt.Errorf("logging sync mode: %v", err)
	}
	var modestr string
	switch mode {
	case dsync.InitialSync:
		modestr = "initial"
	case dsync.Resync:
		modestr = "resync"
	default:
		return nil
	}
	log.Info("synchronizing source %q (%s)", source, modestr)
	return nil
}

func launchPollLoop(ctx context.Context, cat *catalog.Catalog, svr *server, spr *sproc) (reterr error) {
	defer func() {
		if r := recover(); r != nil {
			reterr = fmt.Errorf("%v", r)
			log.Error("%s", reterr)
			// Log stack trace.
			buf := make([]byte, 65536)
			n := runtime.Stack(buf, true)
			log.Detail("%s", buf[0:n])
		}
	}()
	reterr = outerPollLoop(ctx, cat, svr, spr)
	if reterr != nil {
		panic(reterr.Error())
	}
	return
}

func outerPollLoop(ctx context.Context, cat *catalog.Catalog, svr *server, spr *sproc) error {
	var err error
	// Set up source log
	if svr.opt.LogSource != "" {
		if spr.sourceLog, err = log.NewSourceLog(svr.opt.LogSource); err != nil {
			return err
		}
	}

	//// TMP
	// set command.FolioTenant
	/*	var folioTenant string
		if folioTenant, err = catalog.FolioTenant(svr.db); err != nil {
			return err
		}
		command.FolioTenant = folioTenant
	*/
	// set command.ReshareTenants
	command.ReshareTenants, err = catalog.Origins(svr.db)
	if err != nil {
		return err
	}
	////

	log.Debug("starting stream processor")
	if err = pollLoop(ctx, cat, spr); err != nil {
		//log.Error("%s", err)
		return err
	}
	return nil
}

func pollLoop(ctx context.Context, cat *catalog.Catalog, spr *sproc) error {
	// var database0 *sysdb.DatabaseConnector = spr.databases[0]
	//if database0.Type == "postgresql" && database0.DBPort == "" {
	//	database0.DBPort = "5432"
	//}
	//dsn := &sqlx.DSN{
	//	// DBURI: spr.svr.dburi,
	//	Host:     spr.svr.db.Host,
	//	Port:     "5432",
	//	User:     spr.svr.db.User,
	//	Password: spr.svr.db.Password,
	//	DBName:   spr.svr.db.DBName,
	//	SSLMode:  "require",
	//	// Account:  database0.DBAccount,
	//}
	//db, err := sqlx.Open("postgresql", dsn)
	//if err != nil {
	//	return err
	//}
	//// Ping database to test connection
	//if err = db.Ping(); err != nil {
	//	spr.databases[0].Status.Error()
	//	return fmt.Errorf("connecting to database: ping: %s", err)
	//}
	//////////////////////////////////////////////////////////////////////////////
	dc, err := spr.svr.db.Connect()
	if err != nil {
		return err
	}
	dcsuper, err := spr.svr.db.ConnectSuper()
	if err != nil {
		return err
	}
	//////////////////////////////////////////////////////////////////////////////
	spr.databases[0].Status.Active()
	//spr.db = append(spr.db, db)
	// Cache tracking
	//if err = metadata.Init(spr.svr.db, spr.svr.opt.MetadbVersion); err != nil {
	//	return err
	//}
	// Cache schema
	/*	schema, err := cache.NewSchema(db, cat)
		if err != nil {
			return fmt.Errorf("caching schema: %s", err)
		}
	*/
	// Update user permissions in database
	var waitUserPerms sync.WaitGroup
	waitUserPerms.Add(1)
	go func(trackedTables []dbx.Table) {
		defer waitUserPerms.Done()
		var dc2 *pgx.Conn
		dc2, err = spr.svr.db.Connect()
		if err != nil {
			log.Error("%v", err)
		}
		sysdb.GoUpdateUserPerms(dc2, dcsuper, trackedTables)
	}(cat.AllTables(spr.source.Name))
	// Cache users
	/*	users, err := cache.NewUsers(db)
		if err != nil {
			return fmt.Errorf("caching users: %s", err)
		}
	*/
	// Read sync mode from the database.
	syncMode, err := dsync.ReadSyncMode(dc, spr.source.Name)
	if err != nil {
		log.Error("unable to read sync mode: %v", err)
	}

	// Source file
	var sourceFile *os.File
	var sourceFileScanner *bufio.Scanner
	if spr.svr.opt.SourceFilename != "" {
		if sourceFile, err = os.Open(spr.svr.opt.SourceFilename); err != nil {
			return err
		}
		defer func(sourceFile *os.File) {
			_ = sourceFile.Close()
		}(sourceFile)
		sourceFileScanner = bufio.NewScanner(sourceFile)
		sourceFileScanner.Buffer(make([]byte, 0, 10000000), 10000000)
	}
	// Kafka source
	var consumer *kafka.Consumer
	if sourceFileScanner == nil {
		spr.schemaPassFilter, err = util.CompileRegexps(spr.source.SchemaPassFilter)
		if err != nil {
			return err
		}
		spr.schemaStopFilter, err = util.CompileRegexps(spr.source.SchemaStopFilter)
		if err != nil {
			return err
		}
		spr.tableStopFilter, err = util.CompileRegexps(spr.source.TableStopFilter)
		if err != nil {
			return err
		}
		var brokers = spr.source.Brokers
		var topics = spr.source.Topics
		var group = spr.source.Group
		log.Debug("connecting to %q, topics %q", brokers, topics)
		log.Debug("connecting to source %q", spr.source.Name)
		var config = &kafka.ConfigMap{
			"auto.offset.reset":    "earliest",
			"bootstrap.servers":    brokers,
			"enable.auto.commit":   false,
			"group.id":             group,
			"max.poll.interval.ms": spr.svr.db.MaxPollInterval,
			"security.protocol":    spr.source.Security,
		}
		consumer, err = kafka.NewConsumer(config)
		if err != nil {
			spr.source.Status.Error()
			return err
		}
		defer func(consumer *kafka.Consumer) {
			_ = consumer.Close()
		}(consumer)
		//err = consumer.SubscribeTopics([]string{"^" + topicPrefix + "[.].*"}, nil)
		err = consumer.SubscribeTopics(topics, nil)
		if err != nil {
			spr.source.Status.Error()
			return err
		}
		spr.source.Status.Active()
	}
	waitUserPerms.Wait()
	// dedup keeps track of "primary key not defined" and similar errors
	// that have been logged, in order to reduce duplication of the error
	// messages.
	dedup := log.NewMessageSet()
	var firstEvent = true
	for {
		cmdgraph := command.NewCommandGraph()

		// Parse
		eventReadCount, err := parseChangeEvents(cat, dedup, consumer, cmdgraph, spr.schemaPassFilter,
			spr.schemaStopFilter, spr.tableStopFilter, spr.source.TrimSchemaPrefix,
			spr.source.AddSchemaPrefix, sourceFileScanner, spr.sourceLog, spr.svr.db.CheckpointSegmentSize)
		if err != nil {
			return fmt.Errorf("parser: %v", err)
		}
		if firstEvent {
			firstEvent = false
			log.Debug("receiving data from source %q", spr.source.Name)
		}

		// Rewrite
		if err = rewriteCommandGraph(cmdgraph, spr.svr.opt.RewriteJSON); err != nil {
			return fmt.Errorf("rewriter: %s", err)
		}

		// Execute
		if err = execCommandGraph(ctx, cat, cmdgraph, spr.svr.dp, spr.source.Name, syncMode, dedup); err != nil {
			return fmt.Errorf("executor: %s", err)
		}

		if eventReadCount > 0 && sourceFileScanner == nil && !spr.svr.opt.NoKafkaCommit {
			_, err = consumer.Commit()
			if err != nil {
				e := err.(kafka.Error)
				if e.IsFatal() {
					//return fmt.Errorf("Kafka commit: %v", e)
					log.Warning("Kafka commit: %v", e)
				} else {
					switch e.Code() {
					case kafka.ErrNoOffset:
						log.Debug("Kafka commit: %v", e)
					default:
						log.Info("Kafka commit: %v", e)
					}
				}
			}
		}

		if eventReadCount > 0 {
			log.Debug("checkpoint: events=%d, commands=%d", eventReadCount, cmdgraph.Commands.Len())
		}

		// Check if resync snapshot may have completed.
		if syncMode != dsync.NoSync && spr.source.Status.Get() == status.ActiveStatus && cat.HoursSinceLastSnapshotRecord() > 3.0 {
			msg := fmt.Sprintf("source %q snapshot complete (deadline exceeded); consider running \"metadb endsync\"",
				spr.source.Name)
			if dedup.Insert(msg) {
				log.Info("%s", msg)
			}
			cat.ResetLastSnapshotRecord() // Sync timer.
		}
	}
}

func parseChangeEvents(cat *catalog.Catalog, dedup *log.MessageSet, consumer *kafka.Consumer, cmdgraph *command.CommandGraph, schemaPassFilter, schemaStopFilter, tableStopFilter []*regexp.Regexp, trimSchemaPrefix, addSchemaPrefix string, sourceFileScanner *bufio.Scanner, sourceLog *log.SourceLog, checkpointSegmentSize int) (int, error) {
	kafkaPollTimeout := 100     // Poll timeout in milliseconds.
	pollTimeoutCountLimit := 20 // Maximum allowable number of consecutive poll timeouts.
	pollLoopTimeout := 120.0    // Overall pool loop timeout in seconds.
	snapshot := false
	var eventReadCount int
	pollTimeoutCount := 0
	startTime := time.Now()
	for x := 0; x < checkpointSegmentSize; x++ {
		// Catch the possibility of many poll timeouts between messages, because each
		// poll timeouts takes kafkaPollTimeout ms.  This also provides an overall timeout
		// for the poll loop.
		if time.Since(startTime).Seconds() >= pollLoopTimeout {
			log.Trace("poll timeout")
			break
		}
		var err error
		var msg *kafka.Message
		if sourceFileScanner == nil {
			if msg, err = readChangeEvent(consumer, sourceLog, kafkaPollTimeout); err != nil {
				return 0, fmt.Errorf("reading message from Kafka: %v", err)
			}
			if msg == nil { // Poll timeout is indicated by the nil return.
				pollTimeoutCount++
				if pollTimeoutCount >= pollTimeoutCountLimit {
					break // Prevent processing of a small batch from being delayed.
				} else {
					continue
				}
			} else {
				pollTimeoutCount = 0 // We are only interested in consecutive timeouts.
			}
		}
		eventReadCount++

		var ce *change.Event
		if sourceFileScanner != nil {
			if ce, err = readChangeEventFromFile(sourceFileScanner, sourceLog); err != nil {
				return 0, fmt.Errorf("reading change event from file: %v", err)
			}
			if ce == nil && cmdgraph.Commands.Len() == 0 {
				log.Info("finished processing source file")
				log.Info("shutting down")
				process.SetStop()
				break
			}
		} else {
			ce, err = change.NewEvent(msg)
			if err != nil {
				log.Error("%s", err)
				ce = nil
			}
		}

		c, snap, err := command.NewCommand(dedup, ce, schemaPassFilter, schemaStopFilter, tableStopFilter,
			trimSchemaPrefix, addSchemaPrefix)
		if err != nil {
			log.Debug("%v", *ce)
			return 0, fmt.Errorf("parsing command: %v", err)
		}
		if c == nil {
			continue
		}
		if snap {
			snapshot = true
		}
		_ = cmdgraph.Commands.PushBack(c)
	}
	log.Trace("read %d events", cmdgraph.Commands.Len())
	if snapshot {
		cat.ResetLastSnapshotRecord()
	}
	return eventReadCount, nil
}

func readChangeEventFromFile(sourceFileScanner *bufio.Scanner, sourceLog *log.SourceLog) (*change.Event, error) {
	var err error
	var ok bool
	var header, key, value string
	if ok = sourceFileScanner.Scan(); !ok {
		if sourceFileScanner.Err() == nil {
			return nil, nil
		} else {
			return nil, err
		}
	}
	header = sourceFileScanner.Text()
	if header != "#" {
		return nil, fmt.Errorf("header not found")
	}
	if ok = sourceFileScanner.Scan(); !ok {
		if sourceFileScanner.Err() == nil {
			return nil, fmt.Errorf("incomplete read")
		} else {
			return nil, err
		}
	}
	key = sourceFileScanner.Text()
	if ok = sourceFileScanner.Scan(); !ok {
		if sourceFileScanner.Err() == nil {
			return nil, fmt.Errorf("incomplete read")
		} else {
			return nil, err
		}
	}
	value = sourceFileScanner.Text()
	if sourceLog != nil {
		sourceLog.Log("#")
		sourceLog.Log(key)
		sourceLog.Log(value)
	}
	var ce *change.Event
	var msg = &kafka.Message{
		Value: []byte(value),
		Key:   []byte(key),
	}
	if ce, err = change.NewEvent(msg); err != nil {
		log.Error("%s", err)
		ce = nil
	}
	return ce, nil
}

func readChangeEvent(consumer *kafka.Consumer, sourceLog *log.SourceLog, kafkaPollTimeout int) (*kafka.Message, error) {
	ev := consumer.Poll(kafkaPollTimeout)
	if ev == nil {
		return nil, nil
	}
	switch e := ev.(type) {
	case *kafka.Message:
		msg := e
		if msg != nil { // received message
			if sourceLog != nil {
				sourceLog.Log("#")
				sourceLog.Log(string(msg.Key))
				sourceLog.Log(string(msg.Value))
			}
		}
		return e, nil
	//case kafka.PartitionEOF:
	//	log.Trace("%s", e)
	//	return nil, nil
	case kafka.Error:
		// In general, errors from the Kafka
		// client can be reported and ignored,
		// because the client will
		// automatically try to recover.
		if e.IsFatal() {
			log.Warning("Kafka poll: %v", e)
		} else {
			log.Info("Kafka poll: %v", e)
		}
		// We could take some action if
		// desired:
		//if e.Code() == kafka.ErrAllBrokersDown {
		//        // some action
		//}
	default:
		log.Debug("ignoring: %v", e)
	}
	return nil, nil
}

func logTraceCommand(c *command.Command) {
	var schemaTable string
	if c.SchemaName == "" {
		schemaTable = c.TableName
	} else {
		schemaTable = c.SchemaName + "." + c.TableName
	}
	var pkey = command.PrimaryKeyColumns(c.Column)
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "%s: %s", c.Op, schemaTable)
	if c.Op != command.TruncateOp {
		_, _ = fmt.Fprintf(&b, " (")
		var x int
		var col command.CommandColumn
		for x, col = range pkey {
			if x > 0 {
				_, _ = fmt.Fprintf(&b, ", ")
			}
			_, _ = fmt.Fprintf(&b, "%s=%v", col.Name, col.Data)
		}
		_, _ = fmt.Fprintf(&b, ")")
	}
	log.Trace("%s", b.String())
}

func waitForConfig(svr *server) (*sproc, error) {
	var databases = dbxToConnector(svr.db)
	var sources []*sysdb.SourceConnector
	var ready bool
	var err error
	for {
		sources, ready, err = waitForConfigSource(svr)
		if err != nil {
			return nil, err
		}
		if ready {
			break
		}
	}
	var src *sysdb.SourceConnector
	if svr.opt.SourceFilename == "" {
		src = sources[0]
	} else {
		src = &sysdb.SourceConnector{}
	}
	var spr = &sproc{
		source:    src,
		databases: databases,
		svr:       svr,
	}
	return spr, nil
}

func waitForConfigSource(svr *server) ([]*sysdb.SourceConnector, bool, error) {
	svr.state.mu.Lock()
	defer svr.state.mu.Unlock()

	// var databases []*sysdb.DatabaseConnector
	var sources []*sysdb.SourceConnector
	var err error
	// if databases, err = sysdb.ReadDatabaseConnectors(); err != nil {
	// 	return nil, nil, false, err
	// }
	if sources, err = sysdb.ReadSourceConnectors(svr.db); err != nil {
		return nil, false, err
	}
	if len(sources) > 0 {
		if sources[0].Enable {
			// Reread connectors in case configuration was incomplete.
			if sources, err = sysdb.ReadSourceConnectors(svr.db); err != nil {
				return nil, false, err
			}
			if len(sources) > 0 {
				sources[0].Status.Waiting()
				svr.state.sources = sources
			}
			return sources, true, nil
		}
	}
	if svr.opt.SourceFilename != "" {
		// sources = []*sysdb.SourceConnector{{}}
		time.Sleep(2 * time.Second)
	}
	time.Sleep(2 * time.Second)
	return nil, false, nil
}

func dbxToConnector(db *dbx.DB) []*sysdb.DatabaseConnector {
	var dbcs = make([]*sysdb.DatabaseConnector, 0)
	dbcs = append(dbcs, &sysdb.DatabaseConnector{
		DBHost:          db.Host,
		DBPort:          db.Port,
		DBName:          db.DBName,
		DBAdminUser:     db.User,
		DBAdminPassword: db.Password,
		DBSuperUser:     db.SuperUser,
		DBSuperPassword: db.SuperPassword,
	})
	return dbcs
}
