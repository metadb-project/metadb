package upgrade

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/cache"
	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func Upgrade(opt *option.Upgrade) error {
	// Require that a data directory be specified.
	if opt.Datadir == "" {
		return fmt.Errorf("data directory not specified")
	}
	// Ask for confirmation
	if !opt.Force {
		_, _ = fmt.Fprintf(os.Stderr, "Upgrade %q to Metadb %s? ", opt.Datadir, util.MetadbVersion())
		var confirm string
		_, err := fmt.Scanln(&confirm)
		if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
			return nil
		}
	}
	// Upgrade sysdb.
	var upgraded bool
	up, err := upgradeSysdb(opt.Datadir)
	if err != nil {
		return err
	}
	if up {
		upgraded = true
	}
	// Upgrade databases.
	up, err = upgradeDatabases(opt.Datadir)
	if err != nil {
		return err
	}
	if up {
		upgraded = true
	}
	// Write message if nothing was upgraded.
	if upgraded {
		eout.Info("upgrade completed")
	} else {
		eout.Info("databases are up to date")
	}
	return nil
}

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
	err = upgradeSysdbAll(db, dbversion)
	if err != nil {
		return false, err
	}
	return true, nil
}

func upgradeSysdbAll(db *sql.DB, dbversion int64) error {
	opt := sysopt{}
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
}

func upsys5(opt *sysopt) error {
	err := sysdb.WriteSysdbVersion(5)
	if err != nil {
		return err
	}
	return nil
}

type upsysFunc func(opt *sysopt) error

type sysopt struct{}

// Database upgrades

func upgradeDatabases(datadir string) (bool, error) {
	var upgraded bool
	dbcs, err := sysdb.ReadDatabaseConnectors()
	if err != nil {
		return false, err
	}
	for _, dbc := range dbcs {
		dsn := sqlx.DSN{
			Host:     dbc.DBHost,
			Port:     dbc.DBPort,
			User:     dbc.DBAdminUser,
			Password: dbc.DBAdminPassword,
			DBName:   dbc.DBName,
			SSLMode:  dbc.DBSSLMode,
			Account:  dbc.DBAccount,
		}
		up, err := upgradeDatabase(dbc.Name, dbc.Type, &dsn)
		if err != nil {
			return false, err
		}
		if up {
			upgraded = true
		}
	}
	return upgraded, nil
}

func upgradeDatabase(name string, dbtype string, dsn *sqlx.DSN) (bool, error) {
	db, err := sqlx.Open(name, dbtype, dsn)
	if err != nil {
		return false, err
	}
	defer db.Close()
	dbversion, err := metadata.GetDatabaseVersion(db)
	if err != nil {
		return false, fmt.Errorf("%s: %s", name, err)
	}
	if dbversion == util.DatabaseVersion {
		return false, nil
	}
	if dbversion > util.DatabaseVersion {
		return false, fmt.Errorf("%s: database version incompatible with server (%d > %d)", name, dbversion, util.DatabaseVersion)
	}
	err = upgradeDatabaseAll(name, db, dbversion)
	if err != nil {
		return false, err
	}
	return true, nil
}

func upgradeDatabaseAll(name string, db sqlx.DB, dbversion int64) error {
	opt := dbopt{
		DB:    db,
		CName: name,
	}
	for v := dbversion + 1; v <= util.DatabaseVersion; v++ {
		eout.Info("upgrading: %s: version %d", name, v)
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
	updb5,
}

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
		eout.Info("upgrading: %s: table %s", opt.CName, t.String())
		err = updb5Table(opt, schema, &t)
		if err != nil {
			return err
		}
	}
	err = metadata.WriteDatabaseVersion(opt.DB, 5)
	if err != nil {
		return err
	}
	return nil
}

func updb5Table(opt *dbopt, schema *cache.Schema, table *sqlx.Table) error {
	_ = opt.DB.VacuumAnalyzeTable(table)
	_ = opt.DB.VacuumAnalyzeTable(opt.DB.HistoryTable(table))
	alterColumns := make([]string, 0)
	tableSchema := schema.TableSchema(table)
	for colname, coltype := range tableSchema {
		if coltype.DataType == "character varying" && coltype.CharMaxLen >= 36 {
			uuid, err := updb5UUID(opt.DB, table, colname)
			if err != nil {
				return err
			}
			if uuid {
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

type updbFunc func(opt *dbopt) error

type dbopt struct {
	DB    sqlx.DB
	CName string
}
