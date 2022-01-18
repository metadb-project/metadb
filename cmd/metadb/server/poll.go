package server

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/metadb-project/metadb/cmd/metadb/cache"
	"github.com/metadb-project/metadb/cmd/metadb/change"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func goPollLoop(svr *server) {
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
		err := launchPollLoop(svr, spr)
		if err == nil {
			break
		}
		spr.source.Status.Error()
		spr.databases[0].Status.Error()
		time.Sleep(24 * time.Hour)
	}
}

func launchPollLoop(svr *server, spr *sproc) (reterr error) {
	defer func() {
		if r := recover(); r != nil {
			reterr = fmt.Errorf("%v", r)
			log.Fatal("%s", reterr)
		}
	}()
	reterr = outerPollLoop(svr, spr)
	return
}

func outerPollLoop(svr *server, spr *sproc) error {
	var err error
	// Set up source log
	if svr.opt.LogSource != "" {
		if spr.sourceLog, err = log.NewSourceLog(svr.opt.LogSource); err != nil {
			return err
		}
	}

	//// TMP
	// set command.Tenants
	var tenants string
	if tenants, _, err = sysdb.GetConfig("plug.reshare.tenants"); err != nil {
		return err
	}
	if tenants == "" {
		command.Tenants = []string{}
	} else {
		command.Tenants = util.SplitList(tenants)
	}
	////

	log.Debug("starting stream processor")
	if err = pollLoop(spr); err != nil {
		log.Error("%s", err)
		return err
	}
	return nil
}

func pollLoop(spr *sproc) error {
	var err error
	var database0 *sysdb.DatabaseConnector = spr.databases[0]
	//if database0.Type == "postgresql" && database0.DBPort == "" {
	//	database0.DBPort = "5432"
	//}
	dsn := &sqlx.DSN{
		Host:     database0.DBHost,
		Port:     database0.DBPort,
		User:     database0.DBAdminUser,
		Password: database0.DBAdminPassword,
		DBName:   database0.DBName,
		SSLMode:  database0.DBSSLMode,
		Account:  database0.DBAccount,
	}
	db, err := sqlx.Open(database0.Name, database0.Type, dsn)
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
	if err = metadata.Init(db, spr.svr.opt.MetadbVersion); err != nil {
		return err
	}
	track, err := cache.NewTrack(db)
	if err != nil {
		return fmt.Errorf("caching track: %s", err)
	}
	// Cache schema
	schema, err := cache.NewSchema(db, track)
	if err != nil {
		return fmt.Errorf("caching schema: %s", err)
	}
	// Update user permissions in database
	if err = sysdb.UpdateUserPerms(db, track.All()); err != nil {
		return err
	}
	// Cache users
	users, err := cache.NewUsers()
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
		eventReadCount, err := parseChangeEvents(consumer, cl, spr.schemaPassFilter, spr.source.SchemaPrefix, sourceFileScanner, spr.sourceLog)
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
		if err = execCommandList(cl, spr.db[0], track, schema, users); err != nil {
			return fmt.Errorf("executor: %s", err)
		}

		if eventReadCount > 0 && sourceFileScanner == nil && !spr.svr.opt.NoKafkaCommit {
			_, err = consumer.Commit()
			if err != nil {
				//if err.(kafka.Error).Code() == kafka.ErrNoOffset {
				//        log.Warning("kafka: %s", err)
				//}
				return fmt.Errorf("kafka commit: %s", err)
			}
		}

		if eventReadCount > 0 {
			log.Debug("checkpoint: events=%d, commands=%d", eventReadCount, len(cl.Cmd))
		}
	}
}

func parseChangeEvents(consumer *kafka.Consumer, cl *command.CommandList, schemaPassFilter []*regexp.Regexp,
	schemaPrefix string, sourceFileScanner *bufio.Scanner, sourceLog *log.SourceLog) (int, error) {
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
		c, err := command.NewCommand(ce, schemaPassFilter, schemaPrefix)
		if err != nil {
			return 0, fmt.Errorf("parsing command: %s", err)
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
	sigchan := make(chan os.Signal, 1)
	for {
		select {
		case sig := <-sigchan:
			return nil, false, fmt.Errorf("caught signal %v: terminating", sig)
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

// TODO waitForConfig is not currently using the source-database mapping.
func waitForConfig(svr *server) (*sproc, error) {
	log.Debug("waiting for configuration")
	var err error
	var databases []*sysdb.DatabaseConnector
	var sources []*sysdb.SourceConnector
	for {
		if databases, err = sysdb.ReadDatabaseConnectors(); err != nil {
			return nil, err
		}
		if len(databases) > 0 {
			databases[0].Status.Waiting()
			svr.databases = databases
		}
		if sources, err = sysdb.ReadSourceConnectors(); err != nil {
			return nil, err
		}
		if len(sources) > 0 {
			sources[0].Status.Waiting()
			svr.sources = sources
		}
		if len(databases) > 0 && len(sources) > 0 {
			var dbEnabled, srcEnabled bool
			if dbEnabled, err = sysdb.IsConnectorEnabled(databases[0].Name); err != nil {
				return nil, err
			}
			if srcEnabled, err = sysdb.IsConnectorEnabled("src." + sources[0].Name); err != nil {
				return nil, err
			}
			//var users = strings.TrimSpace(databases[0].DBUsers)
			if dbEnabled && srcEnabled {
				break
			}
		}
		if len(databases) > 0 && svr.opt.SourceFilename != "" {
			sources = []*sysdb.SourceConnector{{}}
			time.Sleep(2 * time.Second)
			var dbEnabled bool
			if dbEnabled, err = sysdb.IsConnectorEnabled(databases[0].Name); err != nil {
				return nil, err
			}
			//var users = strings.TrimSpace(databases[0].DBUsers)
			if dbEnabled {
				break
			}
		}
		time.Sleep(2 * time.Second)
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
