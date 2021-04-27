package util

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/metadb-project/metadb/cmd/metadb/log"
)

var latestDatabaseVersion int64 = 22

func validateDatabaseVersion(databaseVersion int64) error {
	latestVersion := latestDatabaseVersion
	if databaseVersion < 20 {
		return fmt.Errorf(""+
			"incompatible database version: %s\n"+
			"please use LDP 1.1 to upgrade the database before running this upgrade process",
			strconv.FormatInt(databaseVersion, 10))
	}
	if databaseVersion > latestVersion {
		return fmt.Errorf("unknown database version: %s",
			strconv.FormatInt(databaseVersion, 10))
	}
	return nil
}

func selectDatabaseVersion(db *sql.DB) (int64, error) {
	sql := "SELECT database_version FROM dbsystem.main;"
	log.Trace(sql)
	var version int64
	err := db.QueryRowContext(context.TODO(), sql).Scan(&version)
	if err != nil {
		log.Trace("database_version not retrieved")
		return 0, err
	}
	log.Trace("database_version: %v", version)
	return version, nil
}
