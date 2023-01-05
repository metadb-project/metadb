package cat

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

var catalogSchema = "metadb"

func Initialize(db *dbx.DB) error {
	dbc, err := db.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dbc)

	exists, err := catalogSchemaExists(dbc)
	if err != nil {
		return fmt.Errorf("checking if database initialized: %v", err)
	}
	if !exists {
		log.Info("initializing database")
		if err = createCatalogSchema(dbc); err != nil {
			return err
		}
		if err = RevokeCreateOnSchemaPublic(db); err != nil {
			return err
		}
		return nil
	}

	// Check that database version is compatible
	err = checkDatabaseCompatible(dbc)
	if err != nil {
		return err
	}
	return nil
}

func checkDatabaseCompatible(dc *pgx.Conn) error {
	dbversion, err := DatabaseVersion(dc)
	if err != nil {
		return err
	}
	if dbversion != util.DatabaseVersion {
		m := fmt.Sprintf("database incompatible with server (%d != %d)", dbversion, util.DatabaseVersion)
		if dbversion < util.DatabaseVersion {
			m = m + ": upgrade using \"metadb upgrade\""
		}
		return fmt.Errorf("%s", m)
	}
	return nil
}

func DatabaseVersion(dc *pgx.Conn) (int64, error) {
	var dbversion int64
	err := dc.QueryRow(context.TODO(), "SELECT dbversion FROM metadb.init").Scan(&dbversion)
	switch {
	case err == pgx.ErrNoRows:
		return 0, fmt.Errorf("unable to query database version")
	case err != nil:
		return 0, fmt.Errorf("querying database version: %s", err)
	default:
		return dbversion, nil
	}
}

func catalogSchemaExists(dbconn *pgx.Conn) (bool, error) {
	var err error
	var q = "SELECT 1 FROM pg_namespace WHERE nspname='" + catalogSchema + "'"
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

func createCatalogSchema(dc *pgx.Conn) error {
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)

	var q = "CREATE SCHEMA " + catalogSchema
	_, err = tx.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("creating schema: "+catalogSchema+": %v", err)
	}

	// Table init
	_, err = tx.Exec(context.TODO(), ""+
		"CREATE TABLE "+catalogSchema+".init (\n"+
		"    version VARCHAR(80) NOT NULL,\n"+
		"    dbversion INTEGER NOT NULL\n"+
		")")
	if err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".track: %v", err)
	}
	mver := util.MetadbVersionString()
	dbver := strconv.FormatInt(util.DatabaseVersion, 10)
	_, err = tx.Exec(context.TODO(), "INSERT INTO "+catalogSchema+".init (version, dbversion) VALUES ('"+mver+"', "+dbver+")")
	if err != nil {
		return fmt.Errorf("writing to table "+catalogSchema+".init: %v", err)
	}

	// Table auth
	_, err = tx.Exec(context.TODO(), ""+
		"CREATE TABLE "+catalogSchema+".auth ("+
		"    username text PRIMARY KEY,"+
		"    tables text NOT NULL,"+
		"    dbupdated boolean NOT NULL"+
		")")
	if err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".auth: %v", err)
	}
	// Table origin
	_, err = tx.Exec(context.TODO(), ""+
		"CREATE TABLE "+catalogSchema+".origin ("+
		"    name text PRIMARY KEY"+
		")")
	if err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".origin: %v", err)
	}
	// Table source
	_, err = tx.Exec(context.TODO(), ""+
		"CREATE TABLE "+catalogSchema+".source ("+
		"    name text PRIMARY KEY,"+
		"    enable boolean NOT NULL,"+
		"    brokers text,"+
		"    security text,"+
		"    topics text,"+
		"    consumergroup text,"+
		"    schemapassfilter text,"+
		"    schemastopfilter text,"+
		"    trimschemaprefix text,"+
		"    addschemaprefix text,"+
		"    module text"+
		")")
	if err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".source: %v", err)
	}
	// Table track
	_, err = tx.Exec(context.TODO(), ""+
		"CREATE TABLE "+catalogSchema+".track (\n"+
		"    schemaname varchar(63) NOT NULL,\n"+
		"    tablename varchar(63) NOT NULL,\n"+
		"    PRIMARY KEY (schemaname, tablename),\n"+
		"    transformed boolean NOT NULL,\n"+
		"    parentschema varchar(63) NOT NULL,\n"+
		"    parenttable varchar(63) NOT NULL\n"+
		")")
	if err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".track: %v", err)
	}
	/*	// Table userperm
		_, err = tx.Exec(context.TODO(), ""+
			"CREATE TABLE "+catalogSchema+".userperm (\n"+
			"    username TEXT PRIMARY KEY,\n"+
			"    tables TEXT NOT NULL,\n"+
			"    dbupdated BOOLEAN NOT NULL\n"+
			")")
		if err != nil {
			return fmt.Errorf("creating table "+catalogSchema+".userperm: %v", err)
		}
	*/

	if err = tx.Commit(context.TODO()); err != nil {
		return fmt.Errorf("initializing system database: committing changes: %v", err)
	}
	return nil
}

func RevokeCreateOnSchemaPublic(db *dbx.DB) error {
	dcsuper, err := db.ConnectSuper()
	if err != nil {
		return err
	}
	defer dbx.Close(dcsuper)
	if _, err := dcsuper.Exec(context.TODO(), "REVOKE CREATE ON SCHEMA public FROM public"); err != nil {
		return err
	}
	return nil
}
