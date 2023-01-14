package server

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/metadb-project/metadb/cmd/metadb/cache"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/change"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func goPollLoop(cat *catalog.Catalog, svr *server) {
	if svr.opt.NoKafkaCommit {
		log.Info("Kafka commits disabled")
	}
	// For now, we support only one source
	spr, err := waitForConfig(svr)
	if err != nil {
		log.Fatal("%s", err)
		os.Exit(1)
	}
	for {
		err := launchPollLoop(cat, svr, spr)
		if err == nil {
			break
		}
		spr.source.Status.Error()
		spr.databases[0].Status.Error()
		time.Sleep(24 * time.Hour)
	}
}

func launchPollLoop(cat *catalog.Catalog, svr *server, spr *sproc) (reterr error) {
	defer func() {
		if r := recover(); r != nil {
			reterr = fmt.Errorf("%v", r)
			log.Fatal("%s", reterr)
		}
	}()
	reterr = outerPollLoop(cat, svr, spr)
	if reterr != nil {
		panic(reterr.Error())
	}
	return
}

func outerPollLoop(cat *catalog.Catalog, svr *server, spr *sproc) error {
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
	if err = pollLoop(cat, spr); err != nil {
		log.Error("%s", err)
		return err
	}
	return nil
}

func pollLoop(cat *catalog.Catalog, spr *sproc) error {
	var err error
	// var database0 *sysdb.DatabaseConnector = spr.databases[0]
	//if database0.Type == "postgresql" && database0.DBPort == "" {
	//	database0.DBPort = "5432"
	//}
	dsn := &sqlx.DSN{
		// DBURI: spr.svr.dburi,
		Host:     spr.svr.db.Host,
		Port:     "5432",
		User:     spr.svr.db.User,
		Password: spr.svr.db.Password,
		DBName:   spr.svr.db.DBName,
		SSLMode:  "require",
		// Account:  database0.DBAccount,
	}
	db, err := sqlx.Open("postgresql", dsn)
	if err != nil {
		return err
	}
	// Ping database to test connection
	if err = db.Ping(); err != nil {
		spr.databases[0].Status.Error()
		return fmt.Errorf("connecting to database: ping: %s", err)
	}
	//////////////////////////////////////////////////////////////////////////////
	spr.databases[0].Status.Active()
	spr.db = append(spr.db, db)
	// Cache tracking
	//if err = metadata.Init(spr.svr.db, spr.svr.opt.MetadbVersion); err != nil {
	//	return err
	//}
	// Cache schema
	schema, err := cache.NewSchema(db, cat)
	if err != nil {
		return fmt.Errorf("caching schema: %s", err)
	}
	// Update user permissions in database
	if err = sysdb.UpdateUserPerms(db, cat.AllTables()); err != nil {
		return err
	}
	// Cache users
	users, err := cache.NewUsers(db)
	if err != nil {
		return fmt.Errorf("caching users: %s", err)
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
		spr.schemaPassFilter = util.CompileRegexps(spr.source.SchemaPassFilter)
		spr.schemaStopFilter = util.CompileRegexps(spr.source.SchemaStopFilter)
		var brokers = spr.source.Brokers
		var topics = spr.source.Topics
		var group = spr.source.Group
		log.Info("connecting to \"%s\", topics \"%s\"", brokers, topics)
		var config = &kafka.ConfigMap{
			"auto.offset.reset":    "earliest",
			"bootstrap.servers":    brokers,
			"enable.auto.commit":   false,
			"enable.partition.eof": true,
			"group.id":             group,
			// TODO - Slow updates can trigger:
			// Local: Maximum application poll interval (max.poll.interval.ms) exceeded: Application maximum poll interval (900000ms) exceeded by 350ms
			"max.poll.interval.ms": 900000,
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
	var firstEvent = true
	for {
		var cl = &command.CommandList{}

		// Parse
		eventReadCount, err := parseChangeEvents(consumer, cl, spr.schemaPassFilter, spr.schemaStopFilter,
			spr.source.TrimSchemaPrefix, spr.source.AddSchemaPrefix, sourceFileScanner, spr.sourceLog)
		if err != nil {
			return fmt.Errorf("parser: %s", err)
		}
		if firstEvent {
			firstEvent = false
			log.Info("receiving data")
		}

		// Rewrite
		before := len(cl.Cmd)
		if err = rewriteCommandList(cl, spr.svr.opt.RewriteJSON); err != nil {
			return fmt.Errorf("rewriter: %s", err)
		}
		after := len(cl.Cmd)
		if before != after {
			log.Trace("%d commands added by rewrite", after-before)
		}

		// Execute
		if err = execCommandList(cat, cl, spr.db[0], schema, users, spr.source.Name); err != nil {
			return fmt.Errorf("executor: %s", err)
		}

		if eventReadCount > 0 && sourceFileScanner == nil && !spr.svr.opt.NoKafkaCommit {
			_, err = consumer.Commit()
			if err != nil {
				if err.(kafka.Error).IsFatal() {
					return fmt.Errorf("kafka commit: %s", err)
				} else {
					log.Warning("kafka commit: %s", err)
				}
			}
		}

		if eventReadCount > 0 {
			log.Debug("checkpoint: events=%d, commands=%d", eventReadCount, len(cl.Cmd))
		}
	}
}

func parseChangeEvents(consumer *kafka.Consumer, cl *command.CommandList, schemaPassFilter, schemaStopFilter []*regexp.Regexp,
	trimSchemaPrefix, addSchemaPrefix string, sourceFileScanner *bufio.Scanner, sourceLog *log.SourceLog) (int, error) {
	var err error
	var eventReadCount int
	var x int
	for x = 0; x < 10000; x++ {
		var ce *change.Event
		if sourceFileScanner != nil {
			if ce, err = readChangeEventFromFile(sourceFileScanner, sourceLog); err != nil {
				return 0, fmt.Errorf("reading change event from file: %s", err)
			}
			if ce == nil && len(cl.Cmd) == 0 {
				log.Info("finished processing source file")
				log.Info("shutting down")
				process.SetStop()
				break
			}
		} else {
			var partitionEOF bool
			if ce, partitionEOF, err = readChangeEvent(consumer, sourceLog); err != nil {
				return 0, fmt.Errorf("reading change event: %s", err)
			}
			if partitionEOF {
				break
			}
		}
		if ce == nil {
			break
		}
		eventReadCount++
		c, err := command.NewCommand(ce, schemaPassFilter, schemaStopFilter, trimSchemaPrefix, addSchemaPrefix)
		if err != nil {
			return 0, fmt.Errorf("parsing command: %s\n%v", err, *ce)
		}
		if c == nil {
			continue
		}
		cl.Cmd = append(cl.Cmd, *c)
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

func readChangeEvent(consumer *kafka.Consumer, sourceLog *log.SourceLog) (*change.Event, bool, error) {
	var err error
	var hup = make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	for {
		select {
		case sig := <-hup:
			return nil, false, fmt.Errorf("caught signal %v: reloading", sig)
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				// fmt.Printf("%% Message on %s:\n%s\n",
				// e.TopicPartition, string(e.Value))

				// fmt.Printf("%% Message on %s\n", e.TopicPartition)
				// if e.Headers != nil {
				// 	fmt.Printf("%% Headers: %v\n", e.Headers)
				// }

				// msg, ok = ev.(*kafka.Message)
				// if !ok {
				// panic("Message not *kafka.Message")
				// }
				// msg = e
				var msg *kafka.Message = e
				var ce *change.Event
				if msg != nil { // received message
					if sourceLog != nil {
						sourceLog.Log("#")
						sourceLog.Log(string(msg.Key))
						sourceLog.Log(string(msg.Value))
					}
					//if msg.Key != nil {
					//        err := json.Unmarshal(msg.Key, &(ce.Key))
					//        if err != nil {
					//                log.Info("change event key: %s\n%s", err, util.KafkaMessageString(msg))
					//        }
					//}
					//if msg.Value != nil {
					//        err = json.Unmarshal(msg.Value, &(ce.Value))
					//        if err != nil {
					//                log.Info("change event value: %s\n%s", err, util.KafkaMessageString(msg))
					//        }
					//}
					//ce.Message = msg
					if ce, err = change.NewEvent(msg); err != nil {
						log.Error("%s", err)
						ce = nil
					}

					// logReceivedChangeEvent(&ce)
				}
				return ce, false, nil
			case kafka.PartitionEOF:
				log.Trace("%s", e)
				return nil, true, nil
			case kafka.Error:
				// In general, errors from the Kafka
				// client can be reported and ignored,
				// because the client will
				// automatically try to recover.
				log.Error("%v: %v", e.Code(), e)
				// We could take some action if
				// desired:
				//if e.Code() == kafka.ErrAllBrokersDown {
				//        // some action
				//}
			default:
				log.Warning("ignoring: %v", e)
			}
		}
	}
}

func logDebugCommand(c *command.Command) {
	var schemaTable string
	if c.SchemaName == "" {
		schemaTable = c.TableName
	} else {
		schemaTable = c.SchemaName + "." + c.TableName
	}
	var pkey []command.CommandColumn = command.PrimaryKeyColumns(c.Column)
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
	log.Debug("%s", b.String())
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
