package sysdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/metadb/database"
	"github.com/metadb-project/metadb/cmd/metadb/log"
)

func EnableConnector(rq *api.EnableRequest) error {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	// filter enabled or unconfigured connectors
	var disabled []string
	var c string
	for _, c = range rq.Connectors {
		// validate that spec exists in config
		var q = fmt.Sprintf("SELECT 1 FROM config WHERE attr LIKE '%s.%%' LIMIT 1;", c)
		var i int64
		err = db.QueryRowContext(context.TODO(), q).Scan(&i)
		switch {
		case err == sql.ErrNoRows:
			return fmt.Errorf("configuration not found for connector: %s", c)
		case err != nil:
			return fmt.Errorf("reading connection configuration: %s: %s", c, err)
		default:
			// NOP
		}
		// remove from list if already enabled
		var enabled bool
		if enabled, err = isConnectorEnabled(c); err != nil {
			return err
		}
		if !enabled {
			disabled = append(disabled, c)
		}
	}
	if len(disabled) == 0 {
		return nil
	}
	// start txn
	var tx *sql.Tx
	if tx, err = database.MakeTx(db); err != nil {
		return err
	}
	defer tx.Rollback()
	// enable connectors
	for _, c = range disabled {
		log.Info("enabling connector: %s", c)
		var q = fmt.Sprintf(""+
			"INSERT INTO connector (spec, enabled) VALUES ('%s', TRUE)\n"+
			"    ON CONFLICT (spec) DO UPDATE SET enabled = TRUE;", c)
		if _, err = tx.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("enabling: %s: %s", c, err)
		}
	}
	// commit
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("enabling connectors: committing changes: %s", err)
	}
	return nil
}

func IsConnectorEnabled(spec string) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()

	return isConnectorEnabled(spec)
}

func isConnectorEnabled(spec string) (bool, error) {
	var q = fmt.Sprintf("SELECT enabled FROM connector WHERE spec = '%s';", spec)
	var enabled bool
	var err = db.QueryRowContext(context.TODO(), q).Scan(&enabled)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("reading connection enabled state: %s: %s", spec, err)
	default:
		return enabled, nil
	}

}
