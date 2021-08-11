package sysdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/internal/status"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

type DatabaseConnector struct {
	ID              int64
	Name            string
	Type            string
	DBHost          string
	DBPort          string
	DBName          string
	DBAdminUser     string
	DBAdminPassword string
	DBUsers         string
	DBSSLMode       string
	Status          status.Status
}

type SourceConnector struct {
	ID               int64
	Name             string
	Brokers          string
	Topics           []string
	Group            string
	SchemaPassFilter []string
	SchemaPrefix     string
	Databases        []string
	Status           status.Status
}

var sysMu sync.Mutex
var db *sql.DB

var initialized bool

func Init(filename string) error {
	return initSysdb(filename, false)
}

// Init and create: call this instead of Init() when creating a new
// database.
func InitCreate(filename string) error {
	return initSysdb(filename, true)
}

func initSysdb(filename string, create bool) error {
	sysMu.Lock()
	defer sysMu.Unlock()

	var err error
	if initialized {
		return fmt.Errorf("initializing sysdb: already initialized")
	}
	var d *sql.DB
	if create {
		// TODO move this block to a function and defer d.Close()
		if d, err = openDatabase(filename); err != nil {
			return err
		}
		if err = initSchema(d); err != nil {
			return err
		}
		d.Close()
		if err = os.Chmod(filename, util.ModePermRW); err != nil {
			return err
		}
	}
	if d, err = openDatabase(filename); err != nil {
		return err
	}
	db = d
	initialized = true
	return nil
}

func Close() error {
	err := db.Close()
	if err != nil {
		return err
	}
	return nil
}

func openDatabase(filename string) (*sql.DB, error) {
	var err error
	var dsn = "file:" + filename +
		"?_busy_timeout=30000" +
		"&_foreign_keys=on" +
		"&_journal_mode=WAL" +
		"&_locking_mode=NORMAL" +
		"&_synchronous=3"
	var d *sql.DB
	if d, err = sql.Open("sqlite3", dsn); err != nil {
		return nil, err
	}
	return d, err
}

func initSchema(d *sql.DB) error {
	var err error
	var tx *sql.Tx
	if tx, err = sqlx.MakeTx(d); err != nil {
		return err
	}
	defer tx.Rollback()

	var thisSchemaVersion int = 0
	eout.Trace("writing database version: %d", thisSchemaVersion)
	var q = fmt.Sprintf("PRAGMA user_version = %d;", thisSchemaVersion)
	if _, err = tx.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("initializing system database: writing database version: %s", err)
	}

	/*

			eout.Trace("creating schema: connect_database")
			s = "" +
				"CREATE TABLE connect_database (\n" +
				"    id INTEGER PRIMARY KEY,\n" +
				"    name TEXT UNIQUE NOT NULL,\n" +
				"    type TEXT NOT NULL,\n" +
				"    dbhost TEXT NOT NULL,\n" +
				"    dbport TEXT NOT NULL,\n" +
				"    dbname TEXT NOT NULL,\n" +
				"    dbuser TEXT NOT NULL,\n" +
				"    dbpassword TEXT NOT NULL,\n" +
				"    dbsslmode TEXT NOT NULL\n" +
				");"
			if _, err = tx.ExecContext(context.TODO(), s); err != nil {
				return fmt.Errorf("initializing system database: creating schema: connect_database: %s", err)
			}

			eout.Trace("creating schema: connect_source_kafka")
			s = "" +
				"CREATE TABLE connect_source_kafka (\n" +
				"    id INTEGER PRIMARY KEY,\n" +
				"    name TEXT UNIQUE NOT NULL,\n" +
				"    brokers TEXT NOT NULL,\n" +
				"    group_id TEXT NOT NULL,\n" +
				"    schema_prefix TEXT NOT NULL\n" +
				");"
			if _, err = tx.ExecContext(context.TODO(), s); err != nil {
				return fmt.Errorf("initializing system database: creating schema: connect_source_kafka: %s", err)
			}

			eout.Trace("creating schema: connect_source_kafka_topic")
			s = "" +
				"CREATE TABLE connect_source_kafka_topic (\n" +
				"    id INTEGER PRIMARY KEY,\n" +
				"    source_id INTEGER NOT NULL REFERENCES connect_source_kafka (id),\n" +
				"    topic TEXT NOT NULL\n" +
				");"
			if _, err = tx.ExecContext(context.TODO(), s); err != nil {
				return fmt.Errorf("initializing system database: creating schema: connect_source_kafka_topic: %s", err)
			}

			eout.Trace("creating schema: connect_source_kafka_schema_pass_filter")
			s = "" +
				"CREATE TABLE connect_source_kafka_schema_pass_filter (\n" +
				"    id INTEGER PRIMARY KEY,\n" +
				"    source_id INTEGER NOT NULL REFERENCES connect_source_kafka (id),\n" +
				"    schema_pass_filter TEXT NOT NULL\n" +
				");"
			if _, err = tx.ExecContext(context.TODO(), s); err != nil {
				return fmt.Errorf("initializing system database: creating schema: connect_source_kafka_schema_pass_filter: %s", err)
			}

			eout.Trace("creating schema: connect_source_kafka_database")
			s = "" +
				"CREATE TABLE connect_source_kafka_database (\n" +
				"    id INTEGER PRIMARY KEY,\n" +
				"    source_id INTEGER NOT NULL REFERENCES connect_source_kafka (id),\n" +
				"    database_id INTEGER NOT NULL REFERENCES connect_database (id)\n" +
				");"
			if _, err = tx.ExecContext(context.TODO(), s); err != nil {
				return fmt.Errorf("initializing system database: creating schema: connect_source_kafka_database: %s", err)
			}

		eout.Trace("creating schema: relation")
		q = "" +
			"CREATE TABLE relation (\n" +
			// "    rel_id bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,\n" +
			"    rel_schema TEXT,\n" +
			"    rel_name TEXT,\n" +
			"    PRIMARY KEY (rel_schema, rel_name)\n" +
			");"
		if _, err = tx.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("initializing system database: creating schema: relation: %s", err)
		}

		eout.Trace("creating schema: attribute")
		q = "" +
			"CREATE TABLE attribute (\n" +
			// "    attr_id bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,\n" +
			"    rel_schema TEXT,\n" +
			"    rel_name TEXT,\n" +
			"    attr_name TEXT,\n" +
			"    attr_type TEXT,\n" +
			"    attr_type_size bigint,\n" +
			"    pkey smallint,\n" +
			"    PRIMARY KEY (rel_schema, rel_name, attr_name)\n" +
			");"
		_, err = tx.ExecContext(context.TODO(), q)
		if err != nil {
			return fmt.Errorf("initializing system database: creating schema: attribute: %s", err)
		}

	*/

	eout.Trace("creating schema: config")
	q = "" +
		"CREATE TABLE config (\n" +
		"    attr TEXT PRIMARY KEY,\n" +
		"    val TEXT NOT NULL\n" +
		");"
	_, err = tx.ExecContext(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("initializing system database: creating schema: config: %s", err)
	}

	eout.Trace("creating schema: connector")
	q = "" +
		"CREATE TABLE connector (\n" +
		"    spec TEXT PRIMARY KEY,\n" +
		"    enabled BOOLEAN NOT NULL\n" +
		");"
	_, err = tx.ExecContext(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("initializing system database: creating schema: connector: %s", err)
	}

	/*
		eout.Trace("creating schema: track")
		q = "" +
			"CREATE TABLE track (\n" +
			"    schemaname TEXT NOT NULL,\n" +
			"    tablename TEXT NOT NULL,\n" +
			"    PRIMARY KEY (schemaname, tablename)\n" +
			");"
		_, err = tx.ExecContext(context.TODO(), q)
		if err != nil {
			return fmt.Errorf("initializing system database: creating schema: track: %s", err)
		}
	*/

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("initializing system database: committing changes: %s", err)
	}
	return nil
}
