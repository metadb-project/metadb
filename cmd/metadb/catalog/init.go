package catalog

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

var catalogSchema = "metadb"

type Catalog struct {
	mu        sync.Mutex
	tableDir  map[dbx.Table]tableEntry
	partYears map[string]map[int]struct{}
	dc        *pgx.Conn
}

func Initialize(db *dbx.DB) (*Catalog, error) {
	dc, err := db.Connect()
	if err != nil {
		return nil, err
	}

	exists, err := catalogSchemaExists(dc)
	if err != nil {
		dbx.Close(dc)
		return nil, fmt.Errorf("checking if database initialized: %v", err)
	}
	if !exists {
		log.Info("initializing database")
		if err = createCatalogSchema(dc); err != nil {
			dbx.Close(dc)
			return nil, err
		}
		if err = RevokeCreateOnSchemaPublic(db); err != nil {
			dbx.Close(dc)
			return nil, err
		}
	} else {
		// Check that database version is compatible
		err = checkDatabaseCompatible(dc)
		if err != nil {
			dbx.Close(dc)
			return nil, err
		}
	}

	c := &Catalog{
		dc: dc,
	}
	if err := c.initTableDir(); err != nil {
		return nil, err
	}
	if err := c.initPartYears(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Catalog) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	dbx.Close(c.dc)
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

func catalogSchemaExists(dc *pgx.Conn) (bool, error) {
	var err error
	var q = "SELECT 1 FROM pg_namespace WHERE nspname=$1"
	var n int32
	err = dc.QueryRow(context.TODO(), q, catalogSchema).Scan(&n)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

type createTableFunc func(pgx.Tx) error

type systemTableDef struct {
	table  dbx.Table
	create createTableFunc
}

var systemTables = []systemTableDef{
	{table: dbx.Table{S: catalogSchema, T: "auth"}, create: createTableAuth},
	{table: dbx.Table{S: catalogSchema, T: "init"}, create: createTableInit},
	{table: dbx.Table{S: catalogSchema, T: "maintenance"}, create: createTableMaintenance},
	{table: dbx.Table{S: catalogSchema, T: "origin"}, create: createTableOrigin},
	{table: dbx.Table{S: catalogSchema, T: "source"}, create: createTableSource},
	{table: dbx.Table{S: catalogSchema, T: "track"}, create: createTableTrack},
}

func SystemTables() []dbx.Table {
	var tables []dbx.Table
	for _, t := range systemTables {
		tables = append(tables, t.table)
	}
	return tables
}

func createCatalogSchema(dc *pgx.Conn) error {
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)

	log.Trace("creating schema %s", catalogSchema)
	var q = "CREATE SCHEMA " + catalogSchema
	_, err = tx.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("creating schema: "+catalogSchema+": %v", err)
	}

	for _, t := range systemTables {
		log.Trace("creating table %s", t.table)
		if err = t.create(tx); err != nil {
			return fmt.Errorf("creating table %s: %v", t.table, err)
		}
	}

	if err = tx.Commit(context.TODO()); err != nil {
		return fmt.Errorf("initializing system database: committing changes: %v", err)
	}
	return nil
}

func createTableAuth(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".auth (" +
		"username text PRIMARY KEY, " +
		"tables text NOT NULL, " +
		"dbupdated boolean NOT NULL)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".auth: %v", err)
	}
	return nil
}

func createTableInit(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".init (" +
		"version VARCHAR(80) NOT NULL, " +
		"dbversion INTEGER NOT NULL)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".track: %v", err)
	}
	mver := util.MetadbVersionString()
	dbver := strconv.FormatInt(util.DatabaseVersion, 10)
	q = "INSERT INTO " + catalogSchema + ".init (version, dbversion) VALUES ('" + mver + "', " + dbver + ")"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("writing to table "+catalogSchema+".init: %v", err)
	}
	return nil
}

func createTableMaintenance(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".maintenance (" +
		"next_maintenance_time timestamptz)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "INSERT INTO " + catalogSchema + ".maintenance " +
		"(next_maintenance_time) VALUES " +
		"(CURRENT_DATE::timestamptz)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	return nil
}

func createTableOrigin(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".origin (" +
		"name text PRIMARY KEY)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".origin: %v", err)
	}
	return nil
}

func createTableSource(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".source (" +
		"name text PRIMARY KEY, " +
		"enable boolean NOT NULL, " +
		"brokers text, " +
		"security text, " +
		"topics text, " +
		"consumergroup text, " +
		"schemapassfilter text, " +
		"schemastopfilter text, " +
		"trimschemaprefix text, " +
		"addschemaprefix text, " +
		"module text)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".source: %v", err)
	}
	return nil
}

func createTableTrack(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".track (" +
		"schemaname varchar(63) NOT NULL, " +
		"tablename varchar(63) NOT NULL, " +
		"PRIMARY KEY (schemaname, tablename), " +
		"transformed boolean NOT NULL, " +
		"parentschema varchar(63) NOT NULL, " +
		"parenttable varchar(63) NOT NULL)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".track: %v", err)
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
