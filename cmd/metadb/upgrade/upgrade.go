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
	// Ask for confirmation
	if !opt.Force {
		_, _ = fmt.Fprintf(os.Stderr, "Upgrade instance %q to Metadb %s? ", opt.Datadir, util.MetadbVersion)
		var confirm string
		_, err := fmt.Scanln(&confirm)
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
	var err error
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
	// Begin upgrade.
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
}

/*
func updb5(opt *dbopt) error {
	track, err := cache.NewTrack(opt.DB)
	if err != nil {
		return err
	}
	schema, err := cache.NewSchema(opt.DB, track)
	if err != nil {
		return err
	}
	for _, t := range track.All() {
		eout.Info("upgrading: version %d: %s", opt.DBVersion, t.String())
		err = updb5Table(opt, schema, &t)
		if err != nil {
			return err
		}
	}
	err = metadata.WriteDatabaseVersion(opt.DB, nil, 5)
	if err != nil {
		return err
	}
	return nil
}

func updb5Table(opt *dbopt, schema *cache.S, table *sqlx.T) error {
	alterColumns := make([]string, 0)
	tableSchema := schema.TableSchema(table)
	for colname, coltype := range tableSchema {
		if coltype.DataType == "character varying" && coltype.CharMaxLen >= 36 {
			uuid, err := updb5UUID(opt.DB, table, colname)
			if err != nil {
				return err
			}
			uuidh, err := updb5UUID(opt.DB, opt.DB.HistoryTable(table), colname)
			if err != nil {
				return err
			}
			if uuid && uuidh {
				alterColumns = append(alterColumns, "ALTER COLUMN "+colname+" TYPE uuid USING "+colname+"::uuid")
			}
		}
		if coltype.DataType == "json" {
			alterColumns = append(alterColumns, "ALTER COLUMN "+colname+" TYPE jsonb")
		}
	}
	if len(alterColumns) != 0 {
		join := strings.Join(alterColumns, ",")
		q := "ALTER TABLE " + opt.DB.TableSQL(table) + " " + join
		_, err := opt.DB.Exec(nil, q)
		if err != nil {
			return err
		}
		q = "ALTER TABLE " + opt.DB.HistoryTableSQL(table) + " " + join
		_, err = opt.DB.Exec(nil, q)
		if err != nil {
			return err
		}
		_ = opt.DB.VacuumAnalyzeTable(table)
		_ = opt.DB.VacuumAnalyzeTable(opt.DB.HistoryTable(table))
	}
	return nil
}

func updb5UUID(db sqlx.DB, table *sqlx.T, colname string) (bool, error) {
	var count int64
	q := "SELECT count(*) FROM " + db.TableSQL(table) + " WHERE " + colname + " NOT LIKE '________-_________-____-____________'"
	err := db.QueryRow(nil, q).Scan(&count)
	switch {
	case err == sql.ErrNoRows:
		return false, fmt.Errorf("internal error: no rows returned by query %s", q)
	case err != nil:
		return false, fmt.Errorf("error querying table %s: %v", table.String(), err)
	default:
		return count == 0, nil
	}
}

func updb6(opt *dbopt) error {
	track, err := cache.NewTrack(opt.DB)
	if err != nil {
		return err
	}
	schema, err := cache.NewSchema(opt.DB, track)
	if err != nil {
		return err
	}
	for _, t := range track.All() {
		eout.Info("upgrading: version %d: %s", opt.DBVersion, t.String())
		err = updb6Table(opt, schema, &t)
		if err != nil {
			return err
		}
		err = updb6Table(opt, schema, opt.DB.HistoryTable(&t))
		if err != nil {
			return err
		}
	}
	err = metadata.WriteDatabaseVersion(opt.DB, nil, 6)
	if err != nil {
		return err
	}
	return nil
}

func updb6Table(opt *dbopt, schema *cache.S, table *sqlx.T) error {
	q := "ALTER TABLE " + opt.DB.TableSQL(table) + " ADD COLUMN __source varchar(63);"
	_, _ = opt.DB.Exec(nil, q)
	q = "UPDATE " + opt.DB.TableSQL(table) + " SET __source='';"
	_, err := opt.DB.Exec(nil, q)
	if err != nil {
		return err
	}
	q = "ALTER TABLE " + opt.DB.TableSQL(table) + " ALTER COLUMN __source SET NOT NULL;"
	_, err = opt.DB.Exec(nil, q)
	if err != nil {
		return err
	}
	_ = opt.DB.VacuumAnalyzeTable(table)
	return nil
}

func updb7(opt *dbopt) error {
	tx, err := opt.DB.BeginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = opt.DB.Exec(nil, "ALTER TABLE metadb.track ADD COLUMN transformed boolean;")
	if err != nil {
		return err
	}
	_, err = opt.DB.Exec(nil, "UPDATE metadb.track SET transformed = (tablename LIKE '%\\_\\_t');")
	if err != nil {
		return err
	}
	_, err = opt.DB.Exec(nil, "ALTER TABLE metadb.track ALTER COLUMN transformed SET NOT NULL;")
	if err != nil {
		return err
	}
	err = metadata.WriteDatabaseVersion(opt.DB, tx, 7)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
*/

func updb8(opt *dbopt) error {
	// Open database
	dc, err := opt.DB.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)
	// Begin transaction
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
	// Begin transaction
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

	// Begin transaction
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
	// Begin transaction
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

	// Begin transaction
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

	// Begin transaction
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
