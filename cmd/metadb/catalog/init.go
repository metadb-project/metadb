package catalog

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

var catalogSchema = "metadb"

type Catalog struct {
	mu        sync.Mutex
	tableDir  map[dbx.Table]tableEntry
	partYears map[string]map[int]struct{}
	users     map[string]*util.RegexList
	columns   map[sqlx.Column]ColumnType
	dp        *pgxpool.Pool
}

func Initialize(db *dbx.DB, dp *pgxpool.Pool) (*Catalog, error) {
	exists, err := catalogSchemaExists(dp)
	if err != nil {
		return nil, fmt.Errorf("checking if database initialized: %v", err)
	}
	if !exists {
		log.Info("initializing database")
		if err = createCatalogSchema(dp); err != nil {
			return nil, err
		}
		if err = RevokeCreateOnSchemaPublic(db); err != nil {
			return nil, err
		}
	} else {
		// Check that database version is compatible
		if err = checkDatabaseCompatible(dp); err != nil {
			return nil, err
		}
	}

	c := &Catalog{dp: dp}
	if err := c.initTableDir(); err != nil {
		return nil, err
	}
	if err := c.initPartYears(); err != nil {
		return nil, err
	}
	if err := c.initUsers(); err != nil {
		return nil, err
	}
	if err := c.initSchema(); err != nil {
		return nil, err
	}

	return c, nil
}

func checkDatabaseCompatible(dp *pgxpool.Pool) error {
	dbversion, err := DatabaseVersion(dp)
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

func DatabaseVersion(dq dbx.Queryable) (int64, error) {
	var dbversion int64
	err := dq.QueryRow(context.TODO(), "SELECT dbversion FROM metadb.init").Scan(&dbversion)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, fmt.Errorf("unable to query database version")
	case err != nil:
		return 0, fmt.Errorf("querying database version: %s", err)
	default:
		return dbversion, nil
	}
}

func catalogSchemaExists(dp *pgxpool.Pool) (bool, error) {
	var err error
	var q = "SELECT 1 FROM pg_namespace WHERE nspname=$1"
	var n int32
	err = dp.QueryRow(context.TODO(), q, catalogSchema).Scan(&n)
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
	{table: dbx.Table{S: catalogSchema, T: "log"}, create: createTableLog},
	{table: dbx.Table{S: catalogSchema, T: "maintenance"}, create: createTableMaintenance},
	{table: dbx.Table{S: catalogSchema, T: "origin"}, create: createTableOrigin},
	{table: dbx.Table{S: catalogSchema, T: "source"}, create: createTableSource},
	{table: dbx.Table{S: catalogSchema, T: "table_update"}, create: createTableUpdate},
	{table: dbx.Table{S: catalogSchema, T: "track"}, create: createTableTrack},
}

func SystemTables() []dbx.Table {
	var tables []dbx.Table
	for _, t := range systemTables {
		tables = append(tables, t.table)
	}
	return tables
}

func createCatalogSchema(dp *pgxpool.Pool) error {
	tx, err := dp.Begin(context.TODO())
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

func createTableLog(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".log (" +
		"log_time timestamptz(3), " +
		"error_severity text, " +
		"message text" +
		") PARTITION BY RANGE (log_time)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return err
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
		"(CURRENT_DATE::timestamptz + INTERVAL '1 day')"
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

func createTableUpdate(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".table_update (" +
		"schemaname text, " +
		"tablename text, " +
		"PRIMARY KEY (schemaname, tablename), " +
		"updated timestamptz)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".table_update: %v", err)
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

func (c *Catalog) TableUpdatedNow(table dbx.Table) error {
	u := catalogSchema + ".table_update"
	q := "INSERT INTO " + u + "(schemaname,tablename,updated)" +
		"VALUES($1,$2,now())" +
		"ON CONFLICT (schemaname,tablename) DO UPDATE SET updated=now()"
	if _, err := c.dp.Exec(context.TODO(), q, table.S, table.T); err != nil {
		return fmt.Errorf("updating table %s in %s: %v", table, u, err)
	}
	return nil
}

func (c *Catalog) RemoveTableUpdated(table dbx.Table) error {
	u := catalogSchema + ".table_update"
	q := "DELETE FROM " + u + " WHERE schemaname=$1 AND tablename=$2"
	if _, err := c.dp.Exec(context.TODO(), q, table.S, table.T); err != nil {
		return fmt.Errorf("removing table %s from %s: %v", table, u, err)
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
