package sysdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/internal/status"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

type DatabaseConnector struct {
	// ID              int64
	// Name            string
	// Type            string
	DBHost          string
	DBPort          string
	DBName          string
	DBAdminUser     string
	DBAdminPassword string
	DBSuperUser     string
	DBSuperPassword string
	// DBUsers         string
	// DBSSLMode       string
	DBAccount string
	Status    status.Status
}

type SourceConnector struct {
	ID               int64
	Name             string
	Enable           bool
	Brokers          string
	Security         string
	Topics           []string
	Group            string
	SchemaPassFilter []string
	SchemaStopFilter []string
	TableStopFilter  []string
	TrimSchemaPrefix string
	AddSchemaPrefix  string
	Module           string
	Status           status.Status
}

//var sysMu dsync.Mutex
//var db *sql.DB

// Deprecated
func Init(s string) error {
	return nil
}

func InitCreate(connString string) error {
	/*
		sysMu.Lock()
		defer sysMu.Unlock()

		var err error

		var dbconn *pgx.Conn
		if dbconn, err = pgx.Connect(context.TODO(), connString); err != nil {
			return err
		}
		defer dbconn.Close(context.TODO())

		var exists bool
		if exists, err = systemSchemaExists(dbconn); err != nil {
			return fmt.Errorf("checking if database initialized: %v", err)
		}
		if exists {
			return fmt.Errorf("database already initialized")
		}

		if err = createSchema(dbconn); err != nil {
			return err
		}
	*/
	return nil
}

func systemSchemaExists(dbconn *pgx.Conn) (bool, error) {
	var err error
	var q = "SELECT 1 FROM pg_namespace WHERE nspname='metadb'"
	var n int32
	err = dbconn.QueryRow(context.TODO(), q).Scan(&n)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

// func Close() error {
// 	err := db.Close()
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// Deprecated
func sysdbBeginTx(d *sql.DB) (*sql.Tx, error) {
	tx, err := d.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// Deprecated
const OpenOptions = "?_busy_timeout=30000" +
	"&_foreign_keys=on" +
	"&_journal_mode=WAL" +
	"&_locking_mode=NORMAL" +
	"&_synchronous=3"

// func openDatabase(filename string) (*sql.DB, error) {
// 	var err error
// 	var dsn = "file:" + filename + OpenOptions
// 	var d *sql.DB
// 	if d, err = sql.Open("sqlite3", dsn); err != nil {
// 		return nil, err
// 	}
// 	return d, err
// }

func createSchema(dbconn *pgx.Conn) error {
	var tx pgx.Tx
	var err error
	if tx, err = dbconn.Begin(context.TODO()); err != nil {
		return err
	}
	defer tx.Rollback(context.TODO())

	var q = "CREATE SCHEMA metadb"
	_, err = tx.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("creating schema: metadb: %v", err)
	}

	// eout.Trace("writing database version: %d", util.DatabaseVersion)
	// var q = fmt.Sprintf("PRAGMA user_version = %d;", util.DatabaseVersion)
	// if _, err = tx.Exec(context.TODO(), q); err != nil {
	// 	return fmt.Errorf("initializing system database: writing database version: %s", err)
	// }

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

	// eout.Trace("creating schema: config")
	// q = "" +
	// 	"CREATE TABLE metadb.config (\n" +
	// 	"    attr TEXT PRIMARY KEY,\n" +
	// 	"    val TEXT NOT NULL\n" +
	// 	");"
	// _, err = tx.Exec(context.TODO(), q)
	// if err != nil {
	// 	return fmt.Errorf("initializing system database: creating schema: config: %s", err)
	// }

	// eout.Trace("creating schema: userperm")
	// q = "" +
	// 	"CREATE TABLE metadb.userperm (\n" +
	// 	"    username TEXT PRIMARY KEY,\n" +
	// 	"    tables TEXT NOT NULL,\n" +
	// 	"    dbupdated BOOLEAN NOT NULL\n" +
	// 	");"
	// _, err = tx.Exec(context.TODO(), q)
	// if err != nil {
	// 	return fmt.Errorf("initializing system database: creating schema: userperm: %s", err)
	// }

	// eout.Trace("creating schema: connector")
	// q = "" +
	// 	"CREATE TABLE metadb.connector (\n" +
	// 	"    spec TEXT PRIMARY KEY,\n" +
	// 	"    enabled BOOLEAN NOT NULL\n" +
	// 	");"
	// _, err = tx.Exec(context.TODO(), q)
	// if err != nil {
	// 	return fmt.Errorf("initializing system database: creating schema: connector: %s", err)
	// }

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

	if err = tx.Commit(context.TODO()); err != nil {
		return fmt.Errorf("initializing system database: committing changes: %s", err)
	}
	return nil
}

func ValidateSysdbVersion(dbconnstr string) error {
	dbversion, err := getSysdbVersion(dbconnstr)
	if err != nil {
		return err
	}
	if dbversion != util.DatabaseVersion {
		m := fmt.Sprintf("data directory incompatible with server (%d != %d)", dbversion, util.DatabaseVersion)
		if dbversion < util.DatabaseVersion {
			m = m + ": upgrade using \"metadb upgrade\""
		}
		return fmt.Errorf("%s", m)
	}
	return nil
}

func GetSysdbVersion(dbconnstr string) (int64, error) {
	dbversion, err := getSysdbVersion(dbconnstr)
	if err != nil {
		return 0, err
	}
	return dbversion, nil
}

func getSysdbVersion(dbconnstr string) (int64, error) {
	var dbconn *pgx.Conn
	var err error
	if dbconn, err = pgx.Connect(context.TODO(), dbconnstr); err != nil {
		return 0, fmt.Errorf("unable to query database version: %v", err)
	}
	defer dbconn.Close(context.TODO())

	var q = "SELECT 1 FROM pg_class c JOIN pg_namespace ns ON c.relnamespace = ns.oid WHERE ns.nspname='metadb' AND c.relname='init'"
	var init int64
	err = dbconn.QueryRow(context.TODO(), q).Scan(&init)
	switch {
	case err == pgx.ErrNoRows:
		return 0, fmt.Errorf("database not initialized")
	case err != nil:
		return 0, fmt.Errorf("unable to query database version: checking for table metadb.init: %v", err)
	default:
	}

	q = "SELECT dbversion FROM metadb.init"
	var dbversion int64
	err = dbconn.QueryRow(context.TODO(), q).Scan(&dbversion)
	switch {
	case err == pgx.ErrNoRows:
		return 0, fmt.Errorf("unable to query database version: no rows in metadb.init")
	case err != nil:
		return 0, fmt.Errorf("unable to query database version: %v", err)
	default:
		return dbversion, nil
	}
}
