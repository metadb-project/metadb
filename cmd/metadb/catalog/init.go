package catalog

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/types"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

var catalogSchema = "metadb"

type Catalog struct {
	mu                 sync.Mutex
	tableDir           map[dbx.Table]tableEntry
	partYears          map[string]map[int]struct{}
	columns            map[dbx.Column]string
	indexes            map[dbx.Column]struct{}
	origins            []string
	jsonTransform      map[types.JSONPath]string
	lastSnapshotRecord time.Time
	dp                 *pgxpool.Pool
	lz4                bool
}

func Initialize(db *dbx.DB, dp *pgxpool.Pool) (*Catalog, error) {
	exists, err := catalogSchemaExists(dp)
	if err != nil {
		return nil, fmt.Errorf("checking if database initialized: %w", err)
	}
	if !exists {
		log.Info("initializing database")
		if err = setCreateRole(db); err != nil {
			return nil, err
		}
		if err = createCatalogSchema(dp); err != nil {
			return nil, err
		}
		if err = RevokeCreateOnSchemaPublic(db); err != nil {
			return nil, err
		}
		if err = grantCreateOnPublic(db); err != nil {
			return nil, err
		}
	} else {
		// Check that database version is compatible.
		if err = CheckDatabaseCompatible(dp); err != nil {
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
	if err := c.initSchema(); err != nil {
		return nil, err
	}
	if err := c.initIndexes(); err != nil {
		return nil, err
	}
	if err := c.initOrigins(); err != nil {
		return nil, err
	}
	if err := c.initJSON(); err != nil {
		return nil, err
	}
	c.initSnapshot()
	c.lz4 = isLZ4Available(c.dp)

	return c, nil
}

func isLZ4Available(dq dbx.Queryable) bool {
	var c string
	q := "SHOW default_toast_compression"
	err := dq.QueryRow(context.TODO(), q).Scan(&c)
	return err == nil
}

func CheckDatabaseCompatible(dp *pgxpool.Pool) error {
	dbversion, err := DatabaseVersion(dp)
	if err != nil {
		return err
	}
	if dbversion != util.DatabaseVersion {
		m := fmt.Sprintf("database incompatible with server: %d != %d", dbversion, util.DatabaseVersion)
		if dbversion < util.DatabaseVersion {
			m += " (upgrade using \"metadb upgrade\")"
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
	{table: dbx.Table{Schema: catalogSchema, Table: "acl"}, create: createTableACL},
	{table: dbx.Table{Schema: catalogSchema, Table: "auth"}, create: createTableAuth},
	{table: dbx.Table{Schema: catalogSchema, Table: "init"}, create: createTableInit},
	{table: dbx.Table{Schema: catalogSchema, Table: "log"}, create: createTableLog},
	{table: dbx.Table{Schema: catalogSchema, Table: "maintenance"}, create: createTableMaintenance},
	{table: dbx.Table{Schema: catalogSchema, Table: "origin"}, create: createTableOrigin},
	{table: dbx.Table{Schema: catalogSchema, Table: "source"}, create: createTableSource},
	{table: dbx.Table{Schema: catalogSchema, Table: "table_update"}, create: createTableUpdate},
	{table: dbx.Table{Schema: catalogSchema, Table: "base_table"}, create: createTableBaseTable},
	{table: dbx.Table{Schema: catalogSchema, Table: "transform_json"}, create: createTableJSON},
}

func PublicSystemTables() []dbx.Table {
	return []dbx.Table{
		{Schema: catalogSchema, Table: "base_table"},
		{Schema: catalogSchema, Table: "log"},
		{Schema: catalogSchema, Table: "table_update"},
	}
}

func IsPublicSystemTable(schema, table string) bool {
	switch schema {
	case catalogSchema:
		switch table {
		case "base_table":
			return true
		case "log":
			return true
		case "table_update":
			return true
		}
	}
	return false
}

func setCreateRole(db *dbx.DB) error {
	dcsuper, err := db.ConnectSuper()
	if err != nil {
		return err
	}
	defer dbx.Close(dcsuper)
	if _, err = dcsuper.Exec(context.TODO(),
		"ALTER USER "+db.User+" CREATEROLE"); err != nil {
		return fmt.Errorf("setting createrole: %w", util.PGErr(err))
	}
	return nil
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
		return fmt.Errorf("creating schema: "+catalogSchema+": %w", err)
	}

	for _, t := range systemTables {
		log.Trace("creating table %s", t.table)
		if err = t.create(tx); err != nil {
			return fmt.Errorf("creating table %s: %v", t.table, err)
		}
	}

	if err = tx.Commit(context.TODO()); err != nil {
		return fmt.Errorf("initializing system database: committing changes: %w", err)
	}
	return nil
}

func createTableACL(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".acl (" +
		"schema_name text NOT NULL, " +
		"object_name text NOT NULL, " +
		"object_type char NOT NULL CHECK (object_type IN ('f', 't')), " +
		"privilege char NOT NULL CHECK (privilege IN ('a')), " +
		"user_name text NOT NULL, " +
		"PRIMARY KEY (schema_name, object_name, object_type, privilege, user_name))"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".acl: %w", err)
	}
	return nil
}

func createTableAuth(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".auth (" +
		"username text PRIMARY KEY)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".auth: %w", err)
	}
	return nil
}

func createTableInit(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".init (" +
		"dbversion integer NOT NULL)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".init: %w", err)
	}
	dbver := strconv.FormatInt(util.DatabaseVersion, 10)
	q = "INSERT INTO " + catalogSchema + ".init (dbversion) VALUES (" + dbver + ")"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("writing to table "+catalogSchema+".init: %w", err)
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
		"(CURRENT_DATE::timestamptz + INTERVAL '1 day' + INTERVAL '3 hours')"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	return nil
}

func createTableOrigin(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".origin (" +
		"name text PRIMARY KEY)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".origin: %w", err)
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
		"tablestopfilter text, " +
		"trimschemaprefix text, " +
		"addschemaprefix text, " +
		"module text, " +
		"sync smallint NOT NULL DEFAULT 1)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".source: %w", err)
	}
	return nil
}

func createTableUpdate(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".table_update (" +
		"schema_name varchar(63), " +
		"table_name varchar(63), " +
		"PRIMARY KEY (schema_name, table_name), " +
		"last_update timestamptz, " +
		"elapsed_real_time real)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".table_update: %w", err)
	}
	return nil
}

func createTableBaseTable(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".base_table (" +
		"schema_name varchar(63) NOT NULL, " +
		"table_name varchar(63) NOT NULL, " +
		"PRIMARY KEY (schema_name, table_name), " +
		"source_name varchar(63) NOT NULL, " +
		"transformed boolean NOT NULL, " +
		"parent_schema_name varchar(63) NOT NULL, " +
		"parent_table_name varchar(63) NOT NULL)"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".base_table: %w", err)
	}
	return nil
}

func createTableJSON(tx pgx.Tx) error {
	q := "CREATE TABLE " + catalogSchema + ".transform_json (" +
		"schema_name varchar(63) NOT NULL, " +
		"table_name varchar(63) NOT NULL, " +
		"column_name varchar(63) NOT NULL, " +
		"path text NOT NULL, " +
		"PRIMARY KEY (schema_name, table_name, column_name, path), " +
		"map text NOT NULL, " +
		"UNIQUE (schema_name, table_name, column_name, map))"
	if _, err := tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table "+catalogSchema+".transform_json: %w", err)
	}
	return nil
}

func (c *Catalog) TableUpdatedNow(table dbx.Table, elapsedTime time.Duration) error {
	realtime := float32(math.Round(elapsedTime.Seconds()*10000) / 10000)
	u := catalogSchema + ".table_update"
	q := "INSERT INTO " + u + "(schema_name,table_name,last_update,elapsed_real_time)" +
		"VALUES($1,$2,now(),$3)" +
		"ON CONFLICT (schema_name,table_name) DO UPDATE SET last_update=now(),elapsed_real_time=$4"
	if _, err := c.dp.Exec(context.TODO(), q, table.Schema, table.Table, realtime, realtime); err != nil {
		return fmt.Errorf("updating table %s in %s: %v", table, u, err)
	}
	return nil
}

func (c *Catalog) RemoveTableUpdated(table dbx.Table) error {
	u := catalogSchema + ".table_update"
	q := "DELETE FROM " + u + " WHERE schema_name=$1 AND table_name=$2"
	if _, err := c.dp.Exec(context.TODO(), q, table.Schema, table.Table); err != nil {
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

func grantCreateOnPublic(db *dbx.DB) error {
	dcsuper, err := db.ConnectSuper()
	if err != nil {
		return err
	}
	defer dbx.Close(dcsuper)
	q := "GRANT CREATE, USAGE ON SCHEMA public TO \"" + db.User + "\""
	if _, err := dcsuper.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("granting systemuser access to public schema: %w", err)
	}
	return nil
}
