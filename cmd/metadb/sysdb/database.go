package sysdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/metadb/database"
)

func ReadDatabaseConnectors() ([]*DatabaseConnector, error) {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	var dc []*DatabaseConnector
	if dc, err = readDatabases(); err != nil {
		return nil, err
	}
	return dc, nil
}

func readDatabases() ([]*DatabaseConnector, error) {
	var rows *sql.Rows
	var err error
	var s = "" +
		"SELECT id, name, type, dbhost, dbport, dbname, dbuser, dbpassword, dbsslmode\n" +
		"    FROM connect_database\n" +
		"    ORDER BY id;"
	if rows, err = db.QueryContext(context.TODO(), s); err != nil {
		return nil, err
	}
	defer rows.Close()
	var dc []*DatabaseConnector
	for rows.Next() {
		var c DatabaseConnector
		if err = rows.Scan(&c.ID, &c.Name, &c.Type, &c.DBHost, &c.DBPort, &c.DBName, &c.DBUser,
			&c.DBPassword, &c.DBSSLMode); err != nil {
			return nil, err
		}
		dc = append(dc, &c)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return dc, nil
}

func UpdateDatabaseConnector(rq api.UpdateDatabaseConnectorRequest) error {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	// Check if source name already exists.
	var s = fmt.Sprintf("SELECT id FROM connect_database WHERE name = '%s';", rq.Name)
	var id int64
	err = db.QueryRowContext(context.TODO(), s).Scan(&id)
	switch {
	case err == sql.ErrNoRows:
		// NOP
	case err != nil:
		return err
	default:
		return fmt.Errorf("modifying a database connector not yet supported")
	}

	// More than one database not currently supported.
	s = fmt.Sprintf("SELECT count(*) FROM connect_database;")
	var count int64
	if err = db.QueryRowContext(context.TODO(), s).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("more than one database connector not currently supported")
	}

	var tx *sql.Tx
	if tx, err = database.MakeTx(db); err != nil {
		return err
	}
	defer tx.Rollback()

	s = fmt.Sprintf(""+
		"INSERT INTO connect_database (\n"+
		"    name, type, dbhost, dbport, dbname, dbuser, dbpassword, dbsslmode\n"+
		") VALUES (\n"+
		"    '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'\n"+
		");", rq.Name, rq.Config.Type, rq.Config.DBHost, rq.Config.DBPort, rq.Config.DBName, rq.Config.DBUser, rq.Config.DBPassword, rq.Config.DBSSLMode,
	)
	if _, err = tx.ExecContext(context.TODO(), s); err != nil {
		return fmt.Errorf("writing database connector: %s: %s", err, s)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("writing database connector: committing changes: %s", err)
	}

	return nil
}
