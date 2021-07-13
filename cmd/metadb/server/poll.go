package server

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/metadb-project/metadb/cmd/metadb/change"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/database"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/process"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func goPollLoop(svr *server) {
	var err error
	if err = outerPollLoop(svr); err != nil {
		log.Fatal("%s", err)
		os.Exit(1)
	}
}

func outerPollLoop(svr *server) error {
	var err error
	if svr.opt.NoKafkaCommit {
		log.Info("Kafka commits disabled")
	}
	// For now, we support only one source
	var spr *sproc
	if spr, err = waitForConfig(svr); err != nil {
		return err
	}
	// Set up source log
	if svr.opt.LogSource != "" {
		if spr.sourceLog, err = log.NewSourceLog(svr.opt.LogSource); err != nil {
			return err
		}
	}

	log.Debug("starting stream processor")
	if err = pollLoop(spr); err != nil {
		log.Error("%s", err)
	}
	return nil
}

func pollLoop(spr *sproc) error {
	var err error
	var database0 *sysdb.DatabaseConnector = spr.databases[0]
	if database0.Type == "postgresql" && database0.DBPort == "" {
		database0.DBPort = "5432"
	}
	var db *sql.DB
	if db, err = database.Open(database0.DBHost, database0.DBPort, database0.DBUser, database0.DBPassword, database0.DBName, database0.DBSSLMode); err != nil {
		return err
	}
	// Ping database to test connection
	if err = db.Ping(); err != nil {
		spr.databases[0].Status.Error()
		return fmt.Errorf("connecting to database: ping: %s", err)
	}
	spr.databases[0].Status.Active()
	spr.db = append(spr.db, db)
	// Source file
	var sourceFile *os.File
	var sourceFileScanner *bufio.Scanner
	if spr.svr.opt.SourceFilename != "" {
		if sourceFile, err = os.Open(spr.svr.opt.SourceFilename); err != nil {
			return err
		}
		defer sourceFile.Close()
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
		consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
			"auto.offset.reset":    "earliest",
			"bootstrap.servers":    brokers,
			"enable.auto.commit":   false,
			"enable.partition.eof": true,
			"group.id":             group,
			// "session.timeout.ms": 6000,
		})
		if err != nil {
			spr.source.Status.Error()
			return err
		}
		defer consumer.Close()
		//err = consumer.SubscribeTopics([]string{"^" + topicPrefix + "[.].*"}, nil)
		err = consumer.SubscribeTopics(topics, nil)
		if err != nil {
			spr.source.Status.Error()
			return err
		}
		spr.source.Status.Active()
	}
	for {
		log.Trace("(poll)")
		var cl = &command.CommandList{}

		// Parse
		if _, err = parseChangeEvents(consumer, cl, spr.schemaPassFilter, spr.source.SchemaPrefix, sourceFileScanner, spr.sourceLog); err != nil {
			////////////////////////////////////////////////////
			log.Error("%s", err)
			if !spr.svr.opt.NoKafkaCommit {
				_, err = consumer.Commit()
				if err != nil {
					log.Error("%s", err)
					panic(err)
				}
			}
			continue
			////////////////////////////////////////////////////
			// return err
		}
		if len(cl.Cmd) == 0 {
			if sourceFileScanner != nil {
				log.Info("finished processing source file")
				log.Info("shutting down")
				process.SetStop()
				return nil
			}
			continue
		}

		/*
			// Rewrite
			if err = rewriteCommandList(cl, svr); err != nil {
				////////////////////////////////////////////////////
				log.Info("skipping non-rewriteable command: %s", err)
				if !svr.opt.NoKafkaCommit {
					_, err = consumer.Commit()
					if err != nil {
						log.Error("%s", err)
						panic(err)
					}
				}
				continue
				////////////////////////////////////////////////////
				// return err
			}
		*/

		// Execute
		if err = execCommandList(cl, spr.db[0]); err != nil {
			////////////////////////////////////////////////////
			log.Error("%s", err)
			if !spr.svr.opt.NoKafkaCommit {
				_, err = consumer.Commit()
				if err != nil {
					log.Error("%s", err)
					panic(err)
				}
			}
			continue
			////////////////////////////////////////////////////
			// return err
		}

		if !spr.svr.opt.NoKafkaCommit {
			_, err = consumer.Commit()
			if err != nil {
				log.Error("%s", err)
				panic(err)
			}
		}

		log.Trace("checkpoint")
		// log.Debug("checkpoint") // log.Debug("checkpoint: %d records", len(cl.Cmd))
	}
}

func parseChangeEvents(consumer *kafka.Consumer, cl *command.CommandList, schemaPassFilter []*regexp.Regexp, schemaPrefix string, sourceFileScanner *bufio.Scanner, sourceLog *log.SourceLog) (int, error) {
	var err error
	var messageCount int
	var x int
	for x = 0; x < 1; x++ {
		var ce *change.Event
		if sourceFileScanner != nil {
			if ce, err = readChangeEventFromFile(sourceFileScanner, sourceLog); err != nil {
				return 0, err
			}
			if ce == nil {
				break
			}
		} else {
			var partitionEOF bool
			if ce, partitionEOF, err = readChangeEvent(consumer, sourceLog); err != nil {
				return 0, err
			}
			if partitionEOF {
				break
			}
		}
		var c *command.Command
		if c, err = command.NewCommand(ce, schemaPassFilter, schemaPrefix); err != nil {
			return 0, err
		}
		if c == nil {
			continue
		}
		messageCount++
		//log.Trace("%#v", c)
		logDebugCommand(c)
		// var txn = &CommandTxn{}
		// txn.Cmd = append(txn.Cmd, *c)
		// cq.Txn = append(cq.Txn, *txn)
		cl.Cmd = append(cl.Cmd, *c)
	}
	return messageCount, nil
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
				fmt.Fprintf(os.Stderr, "%% warning: %v: %v\n", e.Code(), e)
				// We could take some action if
				// desired:
				//if e.Code() == kafka.ErrAllBrokersDown {
				//        // some action
				//}
			default:
				fmt.Fprintf(os.Stderr, "warning: ignored %v\n", e)
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
	var pkey []command.CommandColumn = primaryKeyColumns(c.Column)
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s (", c.Op, schemaTable)
	var x int
	var col command.CommandColumn
	for x, col = range pkey {
		if x > 0 {
			fmt.Fprintf(&b, ", ")
		}
		fmt.Fprintf(&b, "%s=%v", col.Name, col.Data)
	}
	fmt.Fprintf(&b, ")")
	log.Debug("%s", b.String())
}

func primaryKeyColumns(column []command.CommandColumn) []command.CommandColumn {
	var pkey []command.CommandColumn
	var col command.CommandColumn
	for _, col = range column {
		if col.PrimaryKey > 0 {
			pkey = append(pkey, col)
		}
	}
	sort.Slice(pkey, func(i, j int) bool {
		return pkey[i].PrimaryKey > pkey[j].PrimaryKey
	})
	return pkey
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
			break
		}
		if len(databases) > 0 && svr.opt.SourceFilename != "" {
			sources = []*sysdb.SourceConnector{&sysdb.SourceConnector{}}
			time.Sleep(2 * time.Second)
			break
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
