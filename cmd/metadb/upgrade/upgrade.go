package upgrade

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Upgrade(opt *option.Upgrade) error {
	// Require that a data directory be specified.
	if opt.Datadir == "" {
		return fmt.Errorf("data directory not specified")
	}
	// Require that the datadir exists.
	exists, err := util.FileExists(opt.Datadir)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("data directory not found: %s", opt.Datadir)
	}
	// Ask for confirmation
	if !opt.Force {
		_, _ = fmt.Fprintf(os.Stderr, "Upgrade instance %q to Metadb %s? ", opt.Datadir, util.MetadbVersion)
		var confirm string
		_, err = fmt.Scanln(&confirm)
		if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
			return nil
		}
	}
	/*
		// Upgrade sysdb.
		var upgraded bool
		up, err := upgradeSysdb(opt.Datadir)
		if err != nil {
			return err
		}
		if up {
			upgraded = true
		}
	*/
	// Upgrade databases.
	var upgraded bool
	upgraded, err = upgradeDatabase(opt.Datadir)
	if err != nil {
		return err
	}
	// Write message if nothing was upgraded.
	if upgraded {
		eout.Info("upgrade of %q is completed", opt.Datadir)
	} else {
		eout.Info("%q is up to date", opt.Datadir)
	}
	return nil
}

/*

// System upgrades

func upgradeSysdb(datadir string) (bool, error) {
	// Open sysdb.
	dsnsys := "file:" + util.SysdbFileName(datadir) + sysdb.OpenOptions
	db, err := sql.Open("sqlite3", dsnsys)
	if err != nil {
		return false, err
	}
	defer db.Close()
	// begin upgrade.
	dbversion, err := sysdb.GetSysdbVersion()
	if err != nil {
		return false, err
	}
	if dbversion == util.DatabaseVersion {
		return false, nil
	}
	if dbversion > util.DatabaseVersion {
		return false, fmt.Errorf("data directory version incompatible with server (%d > %d)", dbversion, util.DatabaseVersion)
	}
	err = upgradeSysdbAll(db, dbversion, datadir)
	if err != nil {
		return false, err
	}
	return true, nil
}

func upgradeSysdbAll(db *sql.DB, dbversion int64, datadir string) error {
	opt := sysopt{Datadir: datadir}
	for v := dbversion + 1; v <= util.DatabaseVersion; v++ {
		eout.Info("upgrading: system: version %d", v)
		err := upsysList[v](&opt)
		if err != nil {
			return err
		}
	}
	return nil
}

var upsysList = []upsysFunc{
	nil,
	nil,
	nil,
	nil,
	nil,
	upsys5,
	upsys6,
	upsys7,
	upsys8,
}

func upsys5(opt *sysopt) error {
	err := sysdb.WriteSysdbVersion(5)
	if err != nil {
		return err
	}
	return nil
}

func upsys6(opt *sysopt) error {
	err := sysdb.WriteSysdbVersion(6)
	if err != nil {
		return err
	}
	return nil
}

func upsys7(opt *sysopt) error {
	err := sysdb.WriteSysdbVersion(7)
	if err != nil {
		return err
	}
	return nil
}

func upsys8(opt *sysopt) error {
	var err error
	// Read database connection parameters.
	var dbconnectors []*sysdb.DatabaseConnector
	if dbconnectors, err = sysdb.ReadDatabaseConnectors(); err != nil {
		return err
	}
	var dsn *sysdb.DatabaseConnector
	if dsn = dbconnectors[0]; err != nil {
		return err
	}
	var connString = "postgres://" + dsn.DBAdminUser + ":" + dsn.DBAdminPassword + "@" + dsn.DBHost + ":" + dsn.DBPort + "/" + dsn.DBName + "?sslmode=" + dsn.DBSSLMode
	// Write to configuration file.
	var f *os.File
	if f, err = os.Create(util.ConfigFileName(opt.Datadir)); err != nil {
		return fmt.Errorf("creating configuration file: %v", err)
	}
	if _, err = f.WriteString("database = " + connString + "\n"); err != nil {
		return fmt.Errorf("writing configuration file: %v", err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("closing configuration file: %v", err)
	}
	if err = sysdb.WriteSysdbVersion(8); err != nil {
		return err
	}
	return nil
}

type upsysFunc func(opt *sysopt) error

type sysopt struct {
	Datadir string
}

*/

// Database upgrades

/*
func upgradeDatabases(datadir string) (bool, error) {
	var upgraded bool

	host, dbname, superpass, mdbuser, mdbpass, err := util.ReadConfigDatabase(datadir)
	if err != nil {
		return false, err
	}
	var dbcs = make([]*sysdb.DatabaseConnector, 0)
	dbcs = append(dbcs, &sysdb.DatabaseConnector{
		DBHost:          host,
		DBPort:          "5432",
		DBName:          dbname,
		DBAdminUser:     mdbuser,
		DBAdminPassword: mdbpass,
		DBSuperUser:     "postgres",
		DBSuperPassword: superpass,
	})

	// dbcs, err := sysdb.ReadDatabaseConnectors()
	// if err != nil {
	// 	return false, err
	// }
	for _, dbc := range dbcs {
		dsn := sqlx.DSN{
			DBURI: dbc.DBURI,
			// Host:     dbc.DBHost,
			// Port:     dbc.DBPort,
			// User:     dbc.DBAdminUser,
			// Password: dbc.DBAdminPassword,
			// DBName:   dbc.DBName,
			// SSLMode:  dbc.DBSSLMode,
			// Account:  dbc.DBAccount,
		}
		up, err := upgradeDatabase("postgres", &dsn, dburi)
		if err != nil {
			return false, err
		}
		if up {
			upgraded = true
		}
	}
	return upgraded, nil
}
*/

func upgradeDatabase(datadir string) (bool, error) {
	confFileName := util.ConfigFileName(datadir)
	confExists, err := util.FileExists(confFileName)
	if err != nil {
		return false, fmt.Errorf("checking for file %s: %v", confFileName, err)
	}
	//var dbsuper, dbadmin *dbx.DB
	var db *dbx.DB
	if confExists {
		// Read from config file
		db, err = util.ReadConfigDatabase(datadir)
		if err != nil {
			return false, fmt.Errorf("reading configuration file: %v", err)
		}
		/*
			var cfg *ini.File
			cfg, err = ini.Load(confFileName)
			if err != nil {
				return false, fmt.Errorf("reading file %s: %v", confFileName, err)
			}
			s := cfg.Section("main")
			dbsuper = &dbx.DB{
				Host:     s.Key("host").String(),
				Port:     s.Key("port").String(),
				User:     s.Key("superuser").String(),
				Password: s.Key("superuser_password").String(),
				DBName:   s.Key("database").String(),
				SSLMode:  s.Key("sslmode").String(),
			}
			dbadmin = &dbx.DB{
				Host:     s.Key("host").String(),
				Port:     s.Key("port").String(),
				User:     s.Key("systemuser").String(),
				Password: s.Key("systemuser_password").String(),
				DBName:   s.Key("database").String(),
				SSLMode:  s.Key("sslmode").String(),
			}
		*/
	} else {
		// Read from sysdb
		var cs []*databaseConnector
		cs, err = oldReadDatabaseConnectors(datadir)
		if err != nil {
			return false, fmt.Errorf("reading database connectors: %v", err)
		}
		c := cs[0]
		/*
			dbsuper = &dbx.DB{
				Host:     c.DBHost,
				Port:     "5432",
				User:     "postgres",
				Password: c.DBSuperPassword,
				DBName:   c.DBName,
				SSLMode:  "require",
			}
			dbadmin = &dbx.DB{
				Host:     c.DBHost,
				Port:     "5432",
				User:     c.DBAdminUser,
				Password: c.DBAdminPassword,
				DBName:   c.DBName,
				SSLMode:  "require",
			}
		*/
		db = &dbx.DB{
			Host:          c.DBHost,
			Port:          "5432",
			User:          c.DBAdminUser,
			Password:      c.DBAdminPassword,
			SuperUser:     c.DBSuperUser,
			SuperPassword: c.DBSuperPassword,
			DBName:        c.DBName,
			SSLMode:       "require",
		}
		// Create configuration file
		err = createConfigFile(datadir, c)
		if err != nil {
			return false, err
		}
	}

	//dbversion, err := getDatabaseVersion(dbadmin)
	dbversion, err := getDatabaseVersion(db)
	if err != nil {
		return false, fmt.Errorf("%s", err)
	}
	if dbversion == util.DatabaseVersion {
		return false, nil
	}
	if dbversion > util.DatabaseVersion {
		return false, fmt.Errorf("schema version incompatible with server (%d > %d)", dbversion, util.DatabaseVersion)
	}
	//err = upgradeDatabaseAll(datadir, dbsuper, dbadmin, dbversion)
	err = upgradeDatabaseAll(datadir, db, dbversion)
	if err != nil {
		return false, err
	}
	return true, nil
}

func createConfigFile(datadir string, c *databaseConnector) error {
	f, err := os.Create(util.ConfigFileName(datadir))
	if err != nil {
		return fmt.Errorf("creating configuration file: %v", err)
	}
	var s = "[main]\n" +
		"host = " + c.DBHost + "\n" +
		"port = " + c.DBPort + "\n" +
		"database = " + c.DBName + "\n" +
		"superuser = " + c.DBSuperUser + "\n" +
		"superuser_password = " + c.DBSuperPassword + "\n" +
		"systemuser = " + c.DBAdminUser + "\n" +
		"systemuser_password = " + c.DBAdminPassword + "\n" +
		"sslmode = require\n"
	_, err = f.WriteString(s)
	if err != nil {
		return fmt.Errorf("writing configuration file: %v", err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("closing configuration file: %v", err)
	}
	return nil
}

func getDatabaseVersion(db *dbx.DB) (int64, error) {
	dc, err := db.Connect()
	if err != nil {
		return 0, err
	}
	defer dbx.Close(dc)
	dbversion, err := catalog.DatabaseVersion(dc)
	if err != nil {
		return 0, err
	}
	if dbversion < 7 {
		return 0, fmt.Errorf("schema version < 7 not supported")
	}
	return dbversion, err
}

func upgradeDatabaseAll(datadir string, db *dbx.DB, dbversion int64) error {
	for v := dbversion + 1; v <= util.DatabaseVersion; v++ {
		opt := dbopt{
			Datadir:   datadir,
			DB:        db,
			DBVersion: v,
		}
		eout.Info("upgrading: schema version %d", v)
		err := updbList[v](&opt)
		if err != nil {
			return err
		}
	}
	return nil
}

var updbList = []updbFunc{
	nil,
	nil,
	nil,
	nil,
	nil,
	nil,
	nil,
	nil,
	updb8,
	updb9,
	updb10,
	updb11,
	updb12,
	updb13,
	updb14,
	updb15,
	updb16,
	updb17,
	updb18,
	updb19,
	updb20,
	updb21,
	updb22,
	updb23,
}

func updb8(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)
	// begin transaction
	tx, err := dc.BeginTx(context.TODO(), pgx.TxOptions{IsoLevel: "read committed"})
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)

	// Read plug.folio.tenant configuration
	folioTenant, _, err := getConfig(opt.Datadir, "plug.folio.tenant")
	if err != nil {
		return err
	}

	// Read plug.reshare.tenants configuration
	reshareTenantsString, _, err := getConfig(opt.Datadir, "plug.reshare.tenants")
	if err != nil {
		return err
	}
	reshareTenants := strings.Split(reshareTenantsString, ",")
	if len(reshareTenants) == 1 && reshareTenants[0] == "" {
		reshareTenants = []string{}
	}
	q := "CREATE TABLE metadb.origin (" +
		"    name text PRIMARY KEY" +
		")"
	_, err = dc.Exec(context.TODO(), q)
	if err != nil {
		return err
	}
	for _, t := range reshareTenants {
		q = "INSERT INTO metadb.origin (name) VALUES ($1)"
		_, err = dc.Exec(context.TODO(), q, t)
		if err != nil {
			return err
		}
	}

	// Source connector parameters
	sources, err := oldReadSourceConnectors(opt.Datadir, folioTenant, reshareTenants)
	if err != nil {
		return err
	}
	src := sources[0]
	q = "CREATE TABLE metadb.source (" +
		"    name text PRIMARY KEY," +
		"    enable boolean NOT NULL," +
		"    brokers text," +
		"    security text," +
		"    topics text," +
		"    consumergroup text," +
		"    schemapassfilter text," +
		"    trimschemaprefix text," +
		"    addschemaprefix text," +
		"    module text" +
		")"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "INSERT INTO metadb.source (name, enable, brokers, security, topics, consumergroup, schemapassfilter, trimschemaprefix, addschemaprefix, module) " +
		"VALUES ('" + strings.TrimPrefix(src.Name, "src.") + "', TRUE, '" + src.Brokers + "', '" + src.Security + "', '" +
		strings.Join(src.Topics, ",") + "', '" + src.Group + "', '" + strings.Join(src.SchemaPassFilter, ",") + "', '" +
		src.TrimSchemaPrefix + "', '" + src.AddSchemaPrefix + "', '" + src.Module + "')"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}

	// Read users
	users, err := readUsers(opt.Datadir)
	if err != nil {
		return err
	}
	q = "CREATE TABLE metadb.auth (" +
		"    username text PRIMARY KEY," +
		"    tables text NOT NULL," +
		"    dbupdated boolean NOT NULL" +
		")"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	for _, u := range users {
		q = "INSERT INTO metadb.auth (username, tables, dbupdated) VALUES ($1, '.*', TRUE)"
		if _, err = dc.Exec(context.TODO(), q, u); err != nil {
			return err
		}
	}

	// Write new version number
	if err = metadata.WriteDatabaseVersion(tx, 8); err != nil {
		return err
	}
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}

	sysdir := util.SystemDirName(opt.Datadir)
	sysdirOld := sysdir + "_old"
	err = os.Rename(sysdir, sysdirOld)
	if err != nil {
		eout.Warning("renaming %q to %q: %v", sysdir, sysdirOld, err)
	}

	return nil
}

func updb9(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)
	// begin transaction
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)

	q := "ALTER TABLE metadb.source ADD COLUMN schemastopfilter text"
	_, err = dc.Exec(context.TODO(), q)
	if err != nil {
		return err
	}

	// Write new version number
	err = metadata.WriteDatabaseVersion(tx, 9)
	if err != nil {
		return err
	}
	err = tx.Commit(context.TODO())
	if err != nil {
		return err
	}
	return nil
}

func updb10(opt *dbopt) error {
	if err := catalog.RevokeCreateOnSchemaPublic(opt.DB); err != nil {
		return err
	}

	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// Read list of tracked tables.
	q := "SELECT schemaname, tablename FROM metadb.track ORDER BY schemaname, tablename"
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting table list: %v", err)
	}
	type attrschema struct {
		attrname      string
		attrtype      string
		varcharLength int64
	}
	type tableschema struct {
		name  string
		attrs []attrschema
		years []int
	}
	tables := make([]tableschema, 0)
	for rows.Next() {
		var schema, table string
		err = rows.Scan(&schema, &table)
		if err != nil {
			rows.Close()
			return fmt.Errorf("reading table list: %v", err)
		}
		t := tableschema{
			name:  schema + "." + table,
			attrs: make([]attrschema, 0),
			years: make([]int, 0),
		}
		tables = append(tables, t)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("reading table list: %v", err)
	}
	rows.Close()
	// Read table attributes.
	for i, t := range tables {
		q = "SELECT a.attname," +
			" t.typname," +
			" CASE WHEN t.typname='varchar' THEN a.atttypmod-4 ELSE 0 END varchar_length " +
			"FROM pg_class c" +
			" JOIN pg_namespace n ON c.relnamespace=n.oid" +
			" JOIN pg_attribute a ON c.oid=a.attrelid" +
			" JOIN pg_type t ON a.atttypid=t.oid " +
			"WHERE n.nspname||'.'||c.relname=$1" +
			" AND a.attnum>0" +
			" AND a.attname NOT IN" +
			" ('__id', '__cf', '__start', '__end', '__current', '__source', '__origin') " +
			"ORDER BY a.attnum"
		rows, err = dc.Query(context.TODO(), q, t.name)
		if err != nil {
			return fmt.Errorf("selecting attributes: %v", err)
		}
		for rows.Next() {
			var attname, typname string
			var varcharLength int64
			err = rows.Scan(&attname, &typname, &varcharLength)
			if err != nil {
				rows.Close()
				return fmt.Errorf("reading attributes: %v", err)
			}
			a := attrschema{
				attrname:      attname,
				attrtype:      typname,
				varcharLength: varcharLength,
			}
			tables[i].attrs = append(tables[i].attrs, a)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("reading attributes: %v", err)
		}
		rows.Close()
	}
	// Read years from existing rows.
	for i, t := range tables {
		eout.Info("scanning: %s", t.name)
		q = "SELECT DISTINCT EXTRACT(YEAR FROM __start)::smallint AS year FROM " + t.name + "__ ORDER BY year"
		rows, err = dc.Query(context.TODO(), q)
		if err != nil {
			return fmt.Errorf("selecting year list: %v", err)
		}
		for rows.Next() {
			var year int
			err = rows.Scan(&year)
			if err != nil {
				rows.Close()
				return fmt.Errorf("reading year list: %v", err)
			}
			tables[i].years = append(tables[i].years, year)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("reading year list: %v", err)
		}
		rows.Close()
	}

	// begin transaction
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)

	// Upgrade tables.
	for _, t := range tables {
		table := t.name
		htable := table + "__"
		oldhtable := htable + "old___"
		eout.Info("upgrading: %s", table)
		q := "DROP TABLE " + table
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
		q = "ALTER TABLE " + htable + " RENAME TO " + strings.Split(oldhtable, ".")[1]
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
		q = "CREATE TABLE IF NOT EXISTS " + htable + " (" +
			"__id bigint GENERATED BY DEFAULT AS IDENTITY, " +
			"__cf boolean NOT NULL DEFAULT TRUE, " +
			"__start timestamp with time zone NOT NULL, " +
			"__end timestamp with time zone NOT NULL, " +
			"__current boolean NOT NULL, " +
			"__source varchar(63) NOT NULL, " +
			"__origin varchar(63) NOT NULL DEFAULT ''"
		for _, a := range t.attrs {
			q = q + ", \"" + a.attrname + "\" " + a.attrtype
			if a.attrtype == "varchar" {
				q = q + "(" + strconv.FormatInt(a.varcharLength, 10) + ")"
			}
		}
		q = q + ") PARTITION BY LIST (__current)"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
		// Create partitions.
		q = "CREATE TABLE " + table + " PARTITION OF " + htable + " FOR VALUES IN (TRUE)"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
		st := strings.Split(table, ".")
		nctable := st[0] + ".zzz___" + st[1] + "___"
		q = "CREATE TABLE " + nctable + " PARTITION OF " + htable + " FOR VALUES IN (FALSE) " +
			"PARTITION BY RANGE (__start)"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
		for _, year := range t.years {
			yearStr := strconv.Itoa(year)
			start := yearStr + "-01-01"
			end := strconv.Itoa(year+1) + "-01-01"
			q = "CREATE TABLE " + nctable + yearStr + " PARTITION OF " + nctable +
				" FOR VALUES FROM ('" + start + "') TO ('" + end + "')"
			if _, err = dc.Exec(context.TODO(), q); err != nil {
				return err
			}
		}
		// Copy data to new table.
		q = "INSERT INTO " + htable + " (__cf, __start, __end, __current, __source, __origin"
		for _, a := range t.attrs {
			q = q + ", \"" + a.attrname + "\""
		}
		q = q + ") SELECT __cf, __start, __end, __current, __source, __origin"
		for _, a := range t.attrs {
			q = q + ", \"" + a.attrname + "\""
		}
		q = q + " FROM " + oldhtable + " ORDER BY __id"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
		// Drop old table.
		q = "DROP TABLE " + oldhtable
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
		// Create indexes.
		for _, a := range t.attrs {
			if (a.attrtype == "varchar" && a.varcharLength > util.MaximumTypeSizeIndex) ||
				a.attrtype == "jsonb" {
				continue
			}
			q = "CREATE INDEX ON " + htable + " (\"" + a.attrname + "\")"
			if _, err = dc.Exec(context.TODO(), q); err != nil {
				return err
			}
		}
		// Set permissions.
		q = "SELECT username FROM metadb.auth WHERE tables='.*'"
		rows, err = dc.Query(context.TODO(), q)
		if err != nil {
			return fmt.Errorf("selecting user authorizations: %v", err)
		}
		users := make([]string, 0)
		for rows.Next() {
			var username string
			err = rows.Scan(&username)
			if err != nil {
				rows.Close()
				return fmt.Errorf("reading user authorizations: %v", err)
			}
			users = append(users, username)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("reading user authorizations: %v", err)
		}
		rows.Close()
		for _, u := range users {
			q := "GRANT SELECT ON " + htable + " TO " + u
			if _, err = dc.Exec(context.TODO(), q); err != nil {
				return err
			}
			q = "GRANT SELECT ON " + table + " TO " + u
			if _, err = dc.Exec(context.TODO(), q); err != nil {
				return err
			}
		}
	}

	// Write new version number
	err = metadata.WriteDatabaseVersion(tx, 10)
	if err != nil {
		return err
	}
	err = tx.Commit(context.TODO())
	if err != nil {
		return err
	}

	for _, t := range tables {
		eout.Info("vacuuming: %s", t.name)
		q := "VACUUM ANALYZE " + t.name + "__"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
	}

	return nil
}

func updb11(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)
	// begin transaction
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)

	q := "CREATE TABLE metadb.maintenance (" +
		"next_maintenance_time timestamptz" +
		")"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "INSERT INTO metadb.maintenance (next_maintenance_time) " +
		"VALUES (CURRENT_DATE::timestamptz + INTERVAL '1 day')"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}

	// Write new version number
	err = metadata.WriteDatabaseVersion(tx, 11)
	if err != nil {
		return err
	}
	err = tx.Commit(context.TODO())
	if err != nil {
		return err
	}
	return nil
}

func updb12(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	q := "SELECT username FROM metadb.auth"
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return err
	}
	defer rows.Close()
	users := make([]string, 0)
	for rows.Next() {
		var username string
		err = rows.Scan(&username)
		if err != nil {
			return err
		}
		users = append(users, username)
	}
	if err = rows.Err(); err != nil {
		return err
	}

	// begin transaction
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)

	q = "CREATE TABLE metadb.log (" +
		"log_time timestamptz(3), " +
		"error_severity text, " +
		"message text" +
		") PARTITION BY RANGE (log_time)"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}

	q = "CREATE TABLE metadb.table_update (" +
		"schemaname text, " +
		"tablename text, " +
		"PRIMARY KEY (schemaname, tablename), " +
		"updated timestamptz)"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}

	// Write new version number
	err = metadata.WriteDatabaseVersion(tx, 12)
	if err != nil {
		return err
	}
	err = tx.Commit(context.TODO())
	if err != nil {
		return err
	}

	for _, u := range users {
		_, _ = dc.Exec(context.TODO(), "GRANT USAGE ON SCHEMA metadb TO "+u)
		_, _ = dc.Exec(context.TODO(), "GRANT SELECT ON metadb.log TO "+u)
		_, _ = dc.Exec(context.TODO(), "GRANT SELECT ON metadb.table_update TO "+u)
	}

	return nil
}

func updb13(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	q := "SELECT username FROM metadb.auth"
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return err
	}
	defer rows.Close()
	users := make([]string, 0)
	for rows.Next() {
		var username string
		err = rows.Scan(&username)
		if err != nil {
			return err
		}
		users = append(users, username)
	}
	if err = rows.Err(); err != nil {
		return err
	}

	for _, u := range users {
		_, _ = dc.Exec(context.TODO(), "GRANT USAGE ON SCHEMA "+u+" TO "+u+" WITH GRANT OPTION")
		// Redo grants in updb12() that ran on new tables in an uncommitted txn.
		_, _ = dc.Exec(context.TODO(), "GRANT SELECT ON metadb.log TO "+u)
		_, _ = dc.Exec(context.TODO(), "GRANT SELECT ON metadb.table_update TO "+u)
	}

	// begin transaction
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)

	// Write new version number
	err = metadata.WriteDatabaseVersion(tx, 13)
	if err != nil {
		return err
	}
	err = tx.Commit(context.TODO())
	if err != nil {
		return err
	}
	return nil
}

func updb14(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// begin transaction
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)

	q := "ALTER TABLE metadb.source ADD COLUMN tablestopfilter text"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}

	// add resync mode (Add column "resync" to metadb.init and set to false)
	// ok if already in resync?
	q = "ALTER TABLE metadb.init ADD COLUMN resync boolean NOT NULL DEFAULT false"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}

	// Write new version number
	err = metadata.WriteDatabaseVersion(tx, 14)
	if err != nil {
		return err
	}
	err = tx.Commit(context.TODO())
	if err != nil {
		return err
	}
	return nil
}

func updb15(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// Read list of tracked tables.
	q := "SELECT schemaname, tablename FROM metadb.track ORDER BY schemaname, tablename"
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting table list: %v", err)
	}
	tables := make([]dbx.Table, 0)
	for rows.Next() {
		var schema, table string
		err = rows.Scan(&schema, &table)
		if err != nil {
			rows.Close()
			return fmt.Errorf("reading table list: %v", err)
		}
		tables = append(tables, dbx.Table{Schema: schema, Table: table})
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("reading table list: %v", err)
	}
	rows.Close()

	for _, table := range tables {
		q = "SELECT indexname FROM pg_indexes WHERE schemaname=$1 and tablename=$2"
		rows, err = dc.Query(context.TODO(), q, table.Schema, table.Table)
		if err != nil {
			return fmt.Errorf("selecting index list: %v", err)
		}
		indexes := make([]string, 0)
		for rows.Next() {
			var index string
			err = rows.Scan(&index)
			if err != nil {
				rows.Close()
				return fmt.Errorf("reading index list: %v", err)
			}
			indexes = append(indexes, table.Schema+"."+index)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("reading index list: %v", err)
		}
		rows.Close()
		for _, schemaIndex := range indexes {
			q := "DROP INDEX " + schemaIndex
			if _, err = dc.Exec(context.TODO(), q); err != nil {
				return err
			}

		}
	}

	// begin transaction
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	// Write new version number
	err = metadata.WriteDatabaseVersion(tx, 15)
	if err != nil {
		return err
	}
	err = tx.Commit(context.TODO())
	if err != nil {
		return err
	}
	return nil
}

func updb16(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// begin transaction
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	// Add realtime column.
	q := "ALTER TABLE metadb.table_update ADD COLUMN realtime real"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	// Write new version number
	err = metadata.WriteDatabaseVersion(tx, 16)
	if err != nil {
		return err
	}
	err = tx.Commit(context.TODO())
	if err != nil {
		return err
	}
	return nil
}

func updb17(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// Find uuid columns without an index.
	q := `WITH
attr AS (
    SELECT ns.nspname, t.relname, a.attnum, a.attname, y.typname, FALSE AS has_index
        FROM metadb.track AS m
            JOIN pg_class AS t ON m.tablename||'__' = t.relname
            JOIN pg_namespace AS ns ON m.schemaname = ns.nspname AND t.relnamespace = ns.oid
            JOIN pg_attribute AS a ON t.oid = a.attrelid
            JOIN pg_type AS y ON a.atttypid = y.oid
        WHERE t.relkind IN ('r', 'p') AND a.attnum > 0
),
ind AS (
    SELECT d.nspname, d.relname, d.indname, d.attname, d.amname
        FROM ( SELECT ns.nspname,
                      t.relname,
                      i.relname AS indname,
                      a.attname,
                      ( SELECT c.rownum
                            FROM ( SELECT k, row_number() OVER () AS rownum
                                       FROM unnest(x.indkey) WITH ORDINALITY AS a (k)
                                 ) AS c
                            WHERE k = attnum
                      ),
                      am.amname
                   FROM metadb.track AS m
                       JOIN pg_class AS t ON m.tablename||'__' = t.relname
                       JOIN pg_namespace AS ns ON m.schemaname = ns.nspname AND t.relnamespace = ns.oid
                       JOIN pg_index AS x ON t.oid = x.indrelid
                       JOIN pg_class AS i ON x.indexrelid = i.oid
                       JOIN pg_attribute AS a
                           ON t.oid = a.attrelid AND a.attnum = ANY (x.indkey)
                       JOIN pg_opclass AS oc ON x.indclass[0] = oc.oid
                       JOIN pg_am AS am ON oc.opcmethod = am.oid
                   WHERE t.relkind IN ('r', 'p')
                   ORDER BY nspname, relname, indname, rownum
             ) AS d
),
part AS (
    SELECT nspname,
           relname,
           indname,
           first_value(attname) OVER (PARTITION BY nspname, relname, indname) AS attname,
           amname
        FROM ind
),
distpart AS (
    SELECT DISTINCT nspname,
                    relname,
                    attname,
                    TRUE AS has_index,
                    amname
        FROM part
),
joined AS (
    SELECT a.nspname::varchar AS table_schema,
           a.relname::varchar AS table_name,
           a.attname::varchar AS column_name,
           a.attnum AS ordinal_position,
           a.typname AS data_type,
           a.has_index OR coalesce(dp.has_index, FALSE) AS has_index,
           coalesce(dp.amname, '')::varchar AS index_type
        FROM attr AS a
            LEFT JOIN distpart AS dp ON a.nspname = dp.nspname AND a.relname = dp.relname AND a.attname = dp.attname
)
SELECT table_schema, table_name, column_name
    FROM joined
    WHERE data_type = 'uuid' AND NOT has_index AND left(column_name, 2) <> '__'
    ORDER BY table_schema, table_name, column_name`
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting indexes: %v", err)
	}
	defer rows.Close()
	indexes := make([]dbx.Column, 0)
	for rows.Next() {
		var schema, table, column string
		if err = rows.Scan(&schema, &table, &column); err != nil {
			return fmt.Errorf("reading indexes: %v", err)
		}
		indexes = append(indexes, dbx.Column{Schema: schema, Table: table, Column: column})
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("reading indexes: %v", err)
	}

	// Create an index on each uuid column that does not already have an index.
	processed := make(map[dbx.Table]struct{})
	for _, c := range indexes {
		t := dbx.Table{Schema: c.Schema, Table: c.Table}
		_, ok := processed[t]
		if !ok {
			eout.Info("upgrading: table %q", t.String())
			processed[t] = struct{}{}
		}
		q = "CREATE INDEX ON \"" + c.Schema + "\".\"" + c.Table + "\" (\"" + c.Column + "\")"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
	}

	// Remove tables: folio_source_record.marc_records_lb__t__ and folio_source_record.edifact_records_lb__t__
	q = "DELETE FROM metadb.track WHERE schemaname='folio_source_record' AND " +
		"tablename IN ('marc_records_lb__t', 'edifact_records_lb__t')"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "DROP TABLE IF EXISTS folio_source_record.marc_records_lb__t__, folio_source_record.edifact_records_lb__t__"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}

	// Write new version number.
	err = metadata.WriteDatabaseVersion(dc, 17)
	if err != nil {
		return err
	}
	return nil
}

func updb18(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// Find __id columns without an index.
	q := `WITH
attr AS (
    SELECT ns.nspname, t.relname, a.attnum, a.attname, y.typname, FALSE AS has_index
        FROM metadb.track AS m
            JOIN pg_class AS t ON m.tablename||'__' = t.relname
            JOIN pg_namespace AS ns ON m.schemaname = ns.nspname AND t.relnamespace = ns.oid
            JOIN pg_attribute AS a ON t.oid = a.attrelid
            JOIN pg_type AS y ON a.atttypid = y.oid
        WHERE t.relkind IN ('r', 'p') AND a.attnum > 0
),
ind AS (
    SELECT d.nspname, d.relname, d.indname, d.attname, d.amname
        FROM ( SELECT ns.nspname,
                      t.relname,
                      i.relname AS indname,
                      a.attname,
                      ( SELECT c.rownum
                            FROM ( SELECT k, row_number() OVER () AS rownum
                                       FROM unnest(x.indkey) WITH ORDINALITY AS a (k)
                                 ) AS c
                            WHERE k = attnum
                      ),
                      am.amname
                   FROM metadb.track AS m
                       JOIN pg_class AS t ON m.tablename||'__' = t.relname
                       JOIN pg_namespace AS ns ON m.schemaname = ns.nspname AND t.relnamespace = ns.oid
                       JOIN pg_index AS x ON t.oid = x.indrelid
                       JOIN pg_class AS i ON x.indexrelid = i.oid
                       JOIN pg_attribute AS a
                           ON t.oid = a.attrelid AND a.attnum = ANY (x.indkey)
                       JOIN pg_opclass AS oc ON x.indclass[0] = oc.oid
                       JOIN pg_am AS am ON oc.opcmethod = am.oid
                   WHERE t.relkind IN ('r', 'p')
                   ORDER BY nspname, relname, indname, rownum
             ) AS d
),
part AS (
    SELECT nspname,
           relname,
           indname,
           first_value(attname) OVER (PARTITION BY nspname, relname, indname) AS attname,
           amname
        FROM ind
),
distpart AS (
    SELECT DISTINCT nspname,
                    relname,
                    attname,
                    TRUE AS has_index,
                    amname
        FROM part
),
joined AS (
    SELECT a.nspname::varchar AS table_schema,
           a.relname::varchar AS table_name,
           a.attname::varchar AS column_name,
           a.attnum AS ordinal_position,
           a.typname AS data_type,
           a.has_index OR coalesce(dp.has_index, FALSE) AS has_index,
           coalesce(dp.amname, '')::varchar AS index_type
        FROM attr AS a
            LEFT JOIN distpart AS dp ON a.nspname = dp.nspname AND a.relname = dp.relname AND a.attname = dp.attname
)
SELECT table_schema, table_name
    FROM joined
    WHERE data_type = 'int8' AND NOT has_index AND column_name = '__id'
    ORDER BY table_schema, table_name, column_name`
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting indexes: %v", err)
	}
	defer rows.Close()
	indexes := make([]dbx.Table, 0)
	for rows.Next() {
		var schema, table string
		if err = rows.Scan(&schema, &table); err != nil {
			return fmt.Errorf("reading indexes: %v", err)
		}
		indexes = append(indexes, dbx.Table{Schema: schema, Table: table})
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("reading indexes: %v", err)
	}

	// Create an index on each __id column that does not already have an index.
	for _, t := range indexes {
		eout.Info("upgrading: table %q", t.String())
		q = "CREATE INDEX ON \"" + t.Schema + "\".\"" + t.Table + "\" (__id)"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
	}

	// Write new version number.
	err = metadata.WriteDatabaseVersion(dc, 18)
	if err != nil {
		return err
	}
	return nil
}

func updb19(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// Read table names.
	q := `SELECT schemaname, tablename FROM metadb.track ORDER BY schemaname, tablename`
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting table names: %v", err)
	}
	defer rows.Close()
	tables := make([]dbx.Table, 0)
	for rows.Next() {
		var schema, table string
		if err = rows.Scan(&schema, &table); err != nil {
			return fmt.Errorf("reading table names: %v", err)
		}
		tables = append(tables, dbx.Table{Schema: schema, Table: table})
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("reading indexes: %v", err)
	}

	// Drop column "__source" from each table.
	for _, t := range tables {
		q = "ALTER TABLE \"" + t.Schema + "\".\"" + t.Table + "__\" DROP COLUMN IF EXISTS __source"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
	}

	// begin transaction
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	// Add column "source" in table catalog.
	q = "ALTER TABLE metadb.track ADD COLUMN source varchar(63)"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "UPDATE metadb.track SET source = (SELECT name FROM metadb.source LIMIT 1)"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "ALTER TABLE metadb.track ALTER COLUMN source SET NOT NULL"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	// Write new version number.
	if err = metadata.WriteDatabaseVersion(tx, 19); err != nil {
		return err
	}
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	return nil
}

func updb20(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// Find varchar columns.
	q := `SELECT ns.nspname, t.relname, a.attname
    FROM metadb.track AS m
        JOIN pg_class AS t ON m.tablename||'__' = t.relname
        JOIN pg_namespace AS ns ON m.schemaname = ns.nspname AND t.relnamespace = ns.oid
        JOIN pg_attribute AS a ON t.oid = a.attrelid
        JOIN pg_type AS y ON a.atttypid = y.oid
    WHERE t.relkind IN ('r', 'p') AND a.attnum > 0 AND y.typname = 'varchar' AND left(attname, 2) <> '__'
    ORDER BY ns.nspname, t.relname, a.attnum`
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting varchar columns: %v", err)
	}
	defer rows.Close()
	columns := make([]dbx.Column, 0)
	for rows.Next() {
		var schema, table, column string
		if err = rows.Scan(&schema, &table, &column); err != nil {
			return fmt.Errorf("reading varchar columns: %v", err)
		}
		columns = append(columns, dbx.Column{Schema: schema, Table: table, Column: column})
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("reading varchar columns: %v", err)
	}

	// Create an index on each uuid column that does not already have an index.
	processed := make(map[dbx.Table]struct{})
	for _, c := range columns {
		t := dbx.Table{Schema: c.Schema, Table: c.Table}
		_, ok := processed[t]
		if !ok {
			processed[t] = struct{}{}
		}
		q = "ALTER TABLE \"" + c.Schema + "\".\"" + c.Table + "\" ALTER COLUMN \"" + c.Column + "\" TYPE text"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
	}

	// Write new version number.
	err = metadata.WriteDatabaseVersion(dc, 20)
	if err != nil {
		return err
	}
	return nil
}

func updb21(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// begin transaction
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	// Create sync column.
	q := "ALTER TABLE metadb.source ADD COLUMN sync boolean NOT NULL DEFAULT TRUE"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "UPDATE metadb.source SET sync=(SELECT resync FROM metadb.init LIMIT 1)"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	// Remove old resync column.
	q = "ALTER TABLE metadb.init DROP COLUMN resync"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	// Write new version number
	if err = metadata.WriteDatabaseVersion(tx, 21); err != nil {
		return err
	}
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	return nil
}

func updb22(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// Read table names.
	q := `SELECT schemaname, tablename FROM metadb.track ORDER BY schemaname, tablename`
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting table names: %v", err)
	}
	defer rows.Close()
	tables := make([]dbx.Table, 0)
	for rows.Next() {
		var schema, table string
		if err = rows.Scan(&schema, &table); err != nil {
			return fmt.Errorf("reading table names: %v", err)
		}
		tables = append(tables, dbx.Table{Schema: schema, Table: table})
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("reading indexes: %v", err)
	}
	rows.Close()

	for _, t := range tables {
		// Drop column "__cf" from each table.
		q = "ALTER TABLE \"" + t.Schema + "\".\"" + t.Table + "__\" DROP COLUMN IF EXISTS __cf"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
		// Drop old sync table if any.
		synctsql := "\"" + t.Schema + "\".\"zzz___" + t.Table + "___sync\""
		q = "DROP TABLE IF EXISTS " + synctsql
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
		// Create sync table.
		q = "CREATE TABLE " + synctsql + " (__id bigint)"
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
	}

	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	// Change type of dsync column.
	q = "ALTER TABLE metadb.source DROP COLUMN IF EXISTS sync"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "ALTER TABLE metadb.source ADD COLUMN sync smallint NOT NULL DEFAULT 1"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "UPDATE metadb.source SET sync=0"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return err
	}
	// Write new version number.
	err = metadata.WriteDatabaseVersion(tx, 22)
	if err != nil {
		return err
	}
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	return nil
}

func updb23(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	q := "SELECT username FROM metadb.auth"
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return err
	}
	defer rows.Close()
	users := make([]string, 0)
	for rows.Next() {
		var username string
		err = rows.Scan(&username)
		if err != nil {
			return err
		}
		users = append(users, username)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()

	for _, u := range users {
		_, _ = dc.Exec(context.TODO(), "GRANT SELECT ON metadb.track TO "+u)
	}

	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	qs := []string{
		"ALTER TABLE metadb.init DROP version",
		"ALTER TABLE metadb.table_update RENAME schemaname TO schema_name",
		"ALTER TABLE metadb.table_update RENAME tablename TO table_name",
		"ALTER TABLE metadb.table_update RENAME updated TO last_update",
		"ALTER TABLE metadb.table_update RENAME realtime TO elapsed_real_time",
		"ALTER TABLE metadb.table_update ALTER COLUMN schema_name TYPE varchar(63)",
		"ALTER TABLE metadb.table_update ALTER COLUMN table_name TYPE varchar(63)",
		"ALTER TABLE metadb.track RENAME schemaname TO schema_name",
		"ALTER TABLE metadb.track RENAME tablename TO table_name",
		"ALTER TABLE metadb.track RENAME parentschema TO parent_schema_name",
		"ALTER TABLE metadb.track RENAME parenttable TO parent_table_name",
		"ALTER TABLE metadb.track RENAME source TO source_name",
		"ALTER TABLE metadb.track RENAME TO base_table",
	}
	for _, q := range qs {
		if _, err = tx.Exec(context.TODO(), q); err != nil {
			return err
		}
	}
	if err = metadata.WriteDatabaseVersion(tx, 23); err != nil {
		return err
	}
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	return nil
}

//func toPostgresArray(slice []string) string {
//	var b strings.Builder
//	b.WriteString("ARRAY[")
//	var i int
//	var s string
//	for i, s = range slice {
//		if i != 0 {
//			b.WriteRune(',')
//		}
//		b.WriteString("'" + s + "'")
//	}
//	b.WriteString("]::text[]")
//	return b.String()
//}

type updbFunc func(opt *dbopt) error

type dbopt struct {
	Datadir   string
	DB        *dbx.DB
	DBVersion int64
}

//// Old sysdb functions for upgrading from 0.11

func getConfig(datadir string, attr string) (string, bool, error) {
	dsn := "file:" + util.SysdbFileName(datadir) + "?_busy_timeout=30000" +
		"&_foreign_keys=on" +
		"&_journal_mode=WAL" +
		"&_locking_mode=NORMAL" +
		"&_synchronous=3"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return "", false, err
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	// look up config value
	q := fmt.Sprintf(""+
		"SELECT val\n"+
		"    FROM config\n"+
		"    WHERE attr = '%s';", attr)
	var val string
	err = db.QueryRowContext(context.TODO(), q).Scan(&val)
	switch {
	case err == sql.ErrNoRows:
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("reading configuration: %s: %s", attr, err)
	default:
		return val, true, nil
	}
}

func oldReadDatabaseConnectors(datadir string) ([]*databaseConnector, error) {
	var cmap map[string]map[string]string
	var err error
	if cmap, err = readConfigMap(datadir, "db"); err != nil {
		return nil, err
	}
	var dbc []*databaseConnector
	var conf map[string]string
	for _, conf = range cmap {
		dbc = append(dbc, &databaseConnector{
			// Name:            name,
			// Type:            conf["type"],
			DBHost:          conf["host"],
			DBPort:          conf["port"],
			DBName:          conf["dbname"],
			DBAdminUser:     conf["adminuser"],
			DBAdminPassword: conf["adminpassword"],
			DBSuperUser:     conf["superuser"],
			DBSuperPassword: conf["superpassword"],
			// DBUsers:         conf["users"],
			// DBSSLMode:       conf["sslmode"],
			DBAccount: conf["account"],
		})
	}
	return dbc, nil
}

func oldReadSourceConnectors(datadir string, folioTenant string, reshareTenants []string) ([]*sourceConnector, error) {
	if folioTenant == "" && len(reshareTenants) == 0 {
		eout.Warning("neither plug.folio.tenant nor plug.reshare.tenants is defined")
	}
	if folioTenant != "" && len(reshareTenants) != 0 {
		eout.Warning("both plug.folio.tenant and plug.reshare.tenants are defined")
	}
	var trimSchemaPrefix string
	if folioTenant != "" {
		trimSchemaPrefix = folioTenant + "_"
	}
	var module string
	if len(reshareTenants) != 0 {
		module = "reshare"
	} else {
		module = "folio"
	}
	var cmap map[string]map[string]string
	var err error
	if cmap, err = readConfigMap(datadir, "src"); err != nil {
		return nil, err
	}
	var src []*sourceConnector
	var name string
	var conf map[string]string
	for name, conf = range cmap {
		security := conf["security"]
		if security == "" {
			security = "ssl"
		}
		src = append(src, &sourceConnector{
			Name:             name,
			Brokers:          conf["brokers"],
			Security:         security,
			Topics:           util.SplitList(conf["topics"]),
			Group:            conf["group"],
			SchemaPassFilter: util.SplitList(conf["schemapassfilter"]),
			TrimSchemaPrefix: trimSchemaPrefix,
			AddSchemaPrefix:  conf["schemaprefix"],
			Module:           module,
		})
	}
	return src, nil
}

func readConfigMap(datadir string, prefix string) (map[string]map[string]string, error) {
	dsn := "file:" + util.SysdbFileName(datadir) + "?_busy_timeout=30000" +
		"&_foreign_keys=on" +
		"&_journal_mode=WAL" +
		"&_locking_mode=NORMAL" +
		"&_synchronous=3"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	var cmap = make(map[string]map[string]string)
	var rows *sql.Rows
	var q = "SELECT attr, val FROM config WHERE attr LIKE '" + prefix + ".%';"
	if rows, err = db.QueryContext(context.TODO(), q); err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	for rows.Next() {
		var attr, val string
		if err = rows.Scan(&attr, &val); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(attr, prefix+".") {
			continue
		}
		var sp []string = strings.Split(attr, ".")
		if len(sp) < 3 {
			continue
		}
		var name = sp[0] + "." + sp[1]
		var key = sp[2]
		var conf map[string]string
		var ok bool
		if conf, ok = cmap[name]; !ok {
			conf = make(map[string]string)
			cmap[name] = conf
		}
		conf[key] = val
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return cmap, nil
}

func readUsers(datadir string) ([]string, error) {
	dsn := "file:" + util.SysdbFileName(datadir) + "?_busy_timeout=30000" +
		"&_foreign_keys=on" +
		"&_journal_mode=WAL" +
		"&_locking_mode=NORMAL" +
		"&_synchronous=3"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	users := make([]string, 0)
	q := "SELECT username FROM userperm WHERE tables = '.*';"
	rows, err := db.QueryContext(context.TODO(), q)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	for rows.Next() {
		var u string
		if err = rows.Scan(&u); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return users, nil
}

type databaseConnector struct {
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
	// Status    status.Status
}

type sourceConnector struct {
	// ID               int64
	Name             string
	Brokers          string
	Security         string
	Topics           []string
	Group            string
	SchemaPassFilter []string
	TrimSchemaPrefix string
	AddSchemaPrefix  string
	Module           string
	// Status           status.Status
}
