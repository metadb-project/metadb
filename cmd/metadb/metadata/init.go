package metadata

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

// TODO rework
//func Init(db *dbx.DB, metadbVersion string) error {
/*
	dc, err := dbx.Connect(db)
	if err != nil {
		return err
	}
	defer dbx.Close(dc)

	// Check if initialized
	initialized, err := isDatabaseInitialized(dc)
	if err != nil {
		return err
	}
	if initialized {
		// Database already initialized
		// Check that database version is compatible
		err = ValidateDatabaseVersion(dc)
		if err != nil {
			return err
		}
		// Set Metadb version
		_, err = dc.Exec(context.TODO(), "UPDATE metadb.init SET version = '"+util.MetadbVersionString(metadbVersion)+"'")
		if err != nil {
			return fmt.Errorf("updating table metadb.init: %s", err)
		}
		return nil
	}
*/

// moved to catalog/init.go
/*
	// Initialize
	_, err = db.Exec(nil, "CREATE SCHEMA metadb")
	if err != nil {
		return fmt.Errorf("creating schema metadb: %s", err)
	}
	// T metadb.init
	_, err = db.Exec(nil, ""+
		"CREATE TABLE metadb.init (\n"+
		"    version VARCHAR(80) NOT NULL,\n"+
		"    dbversion INTEGER NOT NULL\n"+
		")")
	if err != nil {
		return fmt.Errorf("creating table metadb.track: %s", err)
	}
	mver := metadbVersionString(metadbVersion)
	dbver := strconv.FormatInt(util.DatabaseVersion, 10)
	_, err = db.Exec(nil, "INSERT INTO metadb.init (version, dbversion) VALUES ('"+mver+"', "+dbver+")")
	if err != nil {
		return fmt.Errorf("writing to table metadb.init: %s", err)
	}
	// T metadb.track
	_, err = db.Exec(nil, ""+
		"CREATE TABLE metadb.track (\n"+
		"    schemaname varchar(63) NOT NULL,\n"+
		"    tablename varchar(63) NOT NULL,\n"+
		"    PRIMARY KEY (schemaname, tablename),\n"+
		"    transformed boolean NOT NULL,\n"+
		"    parentschema varchar(63) NOT NULL,\n"+
		"    parenttable varchar(63) NOT NULL\n"+
		")")
	if err != nil {
		return fmt.Errorf("creating table metadb.track: %s", err)
	}
	// T metadb.userperm
	_, err = db.Exec(nil, ""+
		"CREATE TABLE metadb.userperm (\n"+
		"    username TEXT PRIMARY KEY,\n"+
		"    tables TEXT NOT NULL,\n"+
		"    dbupdated BOOLEAN NOT NULL\n"+
		")")
	if err != nil {
		return fmt.Errorf("creating table metadb.userperm: %s", err)
	}
*/

//return nil
//}

/*func isDatabaseInitialized(dc *pgx.Conn) (bool, error) {
	var v int64
	err := dc.QueryRow(context.TODO(), "SELECT dbversion FROM metadb.init LIMIT 1").Scan(&v)
	switch {
	case err == pgx.ErrNoRows:
		return false, fmt.Errorf("reading from table metadb.init: %s", err)
	case err != nil:
		return false, nil
	default:
		return true, nil
	}
}
*/

//func ValidateDatabaseVersion(dc *pgx.Conn) error {
//	dbversion, err := GetDatabaseVersion(dc)
//	if err != nil {
//		return err
//	}
//	if dbversion != util.DatabaseVersion {
//		m := fmt.Sprintf("database incompatible with server (%d != %d)", dbversion, util.DatabaseVersion)
//		if dbversion < util.DatabaseVersion {
//			m = m + ": upgrade using \"metadb upgrade\""
//		}
//		return fmt.Errorf("%s", m)
//	}
//	return nil
//}

func WriteDatabaseVersion(tx pgx.Tx, version int64) error {
	mver := util.MetadbVersionString()
	dbver := strconv.FormatInt(version, 10)
	_, err := tx.Exec(context.TODO(), "UPDATE metadb.init SET version='"+mver+"',dbversion="+dbver)
	if err != nil {
		return fmt.Errorf("updating dbversion in table metadb.init: %s", err)
	}
	return nil
}
