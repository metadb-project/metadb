package upgrade

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/cat"

	"github.com/jackc/pgx/v4"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/util"
	"gopkg.in/ini.v1"
)

func Upgrade(opt *option.Upgrade) error {
	// Require that a data directory be specified.
	if opt.Datadir == "" {
		return fmt.Errorf("data directory not specified")
	}
	// Ask for confirmation
	if !opt.Force {
		_, _ = fmt.Fprintf(os.Stderr, "Upgrade instance %q to Metadb %s? ", opt.Datadir, util.MetadbVersion())
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
		eout.Info("upgrade of instance %q is completed", opt.Datadir)
	} else {
		eout.Info("instance %q is up to date", opt.Datadir)
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
	var dbsuper, dbadmin *dbx.DB
	if confExists {
		// Read from config file
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
	} else {
		// Read from sysdb
		var cs []*databaseConnector
		cs, err = oldReadDatabaseConnectors(datadir)
		if err != nil {
			return false, fmt.Errorf("reading database connectors: %v", err)
		}
		c := cs[0]
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
		// Create configuration file
		f, err := os.Create(util.ConfigFileName(datadir))
		if err != nil {
			return false, fmt.Errorf("creating configuration file: %v", err)
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
			return false, fmt.Errorf("writing configuration file: %v", err)
		}
		err = f.Close()
		if err != nil {
			return false, fmt.Errorf("closing configuration file: %v", err)
		}
	}

	dbversion, err := getDatabaseVersion(dbadmin)
	if err != nil {
		return false, fmt.Errorf("%s", err)
	}
	if dbversion == util.DatabaseVersion {
		return false, nil
	}
	if dbversion > util.DatabaseVersion {
		return false, fmt.Errorf("database version incompatible with server (%d > %d)", dbversion, util.DatabaseVersion)
	}
	err = upgradeDatabaseAll(datadir, dbsuper, dbadmin, dbversion)
	if err != nil {
		return false, err
	}
	return true, nil
}

func getDatabaseVersion(db *dbx.DB) (int64, error) {
	dc, err := dbx.Connect(db)
	if err != nil {
		return 0, err
	}
	defer dbx.Close(dc)
	dbversion, err := cat.DatabaseVersion(dc)
	if err != nil {
		return 0, err
	}
	if dbversion < 7 {
		return 0, fmt.Errorf("version < 7 not supported")
	}
	return dbversion, err
}

func upgradeDatabaseAll(datadir string, dbsuper, dbadmin *dbx.DB, dbversion int64) error {
	for v := dbversion + 1; v <= util.DatabaseVersion; v++ {
		opt := dbopt{
			Datadir:   datadir,
			DBSuper:   dbsuper,
			DBAdmin:   dbadmin,
			DBVersion: v,
		}
		eout.Info("upgrading: version %d", v)
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

func updb5Table(opt *dbopt, schema *cache.Schema, table *sqlx.Table) error {
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

func updb5UUID(db sqlx.DB, table *sqlx.Table, colname string) (bool, error) {
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

func updb6Table(opt *dbopt, schema *cache.Schema, table *sqlx.Table) error {
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
	dc, err := dbx.Connect(opt.DBAdmin)
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

	// Source connector parameters
	sources, err := oldReadSourceConnectors(opt.Datadir)
	if err != nil {
		return err
	}
	src := sources[0]
	var q = "CREATE TABLE metadb.source (" +
		"    name text PRIMARY KEY," +
		"    brokers text NOT NULL," +
		"    security text NOT NULL," +
		"    topics text[] NOT NULL," +
		"    consumergroup text NOT NULL," +
		"    schemapassfilter text[] NOT NULL," +
		"    schemaprefix text NOT NULL" +
		")"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "INSERT INTO metadb.source (name, brokers, security, topics, consumergroup, schemapassfilter, schemaprefix) " +
		"VALUES ('" + strings.TrimPrefix(src.Name, "src.") + "', '" + src.Brokers + "', '" + src.Security + "', " +
		toPostgresArray(src.Topics) + ", '" + src.Group + "', " + toPostgresArray(src.SchemaPassFilter) + ", '" +
		src.SchemaPrefix + "')"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}

	// Read users
	users, err := readUsers(opt.Datadir)
	if err != nil {
		return err
	}
	q = "CREATE TABLE metadb.auth (" +
		"    username text PRIMARY KEY" +
		")"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	for _, u := range users {
		q = "INSERT INTO metadb.auth (username) VALUES ($1)"
		if _, err = dc.Exec(context.TODO(), q, u); err != nil {
			return err
		}
	}

	// Read plug.folio.tenant configuration
	folioTenant, _, err := getConfig(opt.Datadir, "plug.folio.tenant")
	if err != nil {
		return err
	}
	q = "CREATE TABLE metadb.folio (" +
		"    attr text PRIMARY KEY," +
		"    val text NOT NULL" +
		")"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "INSERT INTO metadb.folio (attr, val) VALUES ($1, $2)"
	if _, err = dc.Exec(context.TODO(), q, "tenant", folioTenant); err != nil {
		return err
	}

	// Read plug.reshare.tenants configuration
	reshareTenants, _, err := getConfig(opt.Datadir, "plug.reshare.tenants")
	if err != nil {
		return err
	}
	q = "CREATE TABLE metadb.reshare (" +
		"    attr text PRIMARY KEY," +
		"    val text NOT NULL" +
		")"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	q = "INSERT INTO metadb.reshare (attr, val) VALUES ($1, $2)"
	if _, err = dc.Exec(context.TODO(), q, "tenants", reshareTenants); err != nil {
		return err
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

func toPostgresArray(slice []string) string {
	var b strings.Builder
	b.WriteString("ARRAY[")
	var i int
	var s string
	for i, s = range slice {
		if i != 0 {
			b.WriteRune(',')
		}
		b.WriteString("'" + s + "'")
	}
	b.WriteString("]::text[]")
	return b.String()
}

type updbFunc func(opt *dbopt) error

type dbopt struct {
	Datadir   string
	DBSuper   *dbx.DB
	DBAdmin   *dbx.DB
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

func oldReadSourceConnectors(datadir string) ([]*sourceConnector, error) {
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
			SchemaPrefix:     conf["schemaprefix"],
			// Databases:        util.SplitList(conf["dbs"]),
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
	SchemaPrefix     string
	// Status           status.Status
}
