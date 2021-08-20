package sysdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

func EnableConnector(rq *api.EnableRequest) error {
	sysMu.Lock()
	defer sysMu.Unlock()

	var err error
	// filter enabled or unconfigured connectors
	var conn []string
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
			conn = append(conn, c)
		}
		////
		// TMP check for db.<name>.users
		if strings.HasPrefix(c, "db.") && !strings.HasSuffix(c, ".") {
			attr := c + ".users"
			users, err, _ := getConfig(attr)
			if err != nil {
				return fmt.Errorf("reading configuration: %s: %s", attr, err)
			}
			if strings.TrimSpace(users) == "" {
				return fmt.Errorf("%s.users is undefined", c)
			}
		}
		////
	}
	if len(conn) == 0 {
		return nil
	}
	// start txn
	var tx *sql.Tx
	if tx, err = sqlx.MakeTx(db); err != nil {
		return err
	}
	defer tx.Rollback()
	// enable connectors
	for _, c = range conn {
		log.Info("enabling connector: %s", c)
		var q = fmt.Sprintf(""+
			"INSERT INTO connector (spec,enabled) VALUES ('%s',TRUE)\n"+
			"    ON CONFLICT (spec) DO UPDATE SET enabled=TRUE;", c)
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

func DisableConnector(rq *api.DisableRequest) error {
	sysMu.Lock()
	defer sysMu.Unlock()

	var err error
	// filter disabled or unconfigured connectors
	var conn []string
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
		// remove from list if already disabled
		var enabled bool
		if enabled, err = isConnectorEnabled(c); err != nil {
			return err
		}
		if enabled {
			conn = append(conn, c)
		}
	}
	if len(conn) == 0 {
		return nil
	}
	// start txn
	var tx *sql.Tx
	if tx, err = sqlx.MakeTx(db); err != nil {
		return err
	}
	defer tx.Rollback()
	// enable connectors
	for _, c = range conn {
		log.Info("disabling connector: %s", c)
		var q = fmt.Sprintf(""+
			"INSERT INTO connector (spec,enabled) VALUES ('%s',FALSE)\n"+
			"    ON CONFLICT (spec) DO UPDATE SET enabled=FALSE;", c)
		if _, err = tx.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("disabling: %s: %s", c, err)
		}
	}
	// commit
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("disabling connectors: committing changes: %s", err)
	}
	return nil
}

func IsConnectorEnabled(spec string) (bool, error) {
	sysMu.Lock()
	defer sysMu.Unlock()

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

func DisableSourceConnectors() error {
	sysMu.Lock()
	defer sysMu.Unlock()

	// Disable connectors
	if _, err := db.ExecContext(context.TODO(), "UPDATE connector SET enabled=FALSE WHERE spec LIKE 'src.%'"); err != nil {
		return err
	}
	// Get list of source connectors
	specs, err := sourceConnectors()
	if err != nil {
		return err
	}
	eout.Info("disabled source connectors: %s", strings.Join(specs, " "))
	// Remove topics and group from source config
	for _, spec := range specs {
		// Topics
		topicsAttr := spec + ".topics"
		_, err, ok := getConfig(topicsAttr)
		if err != nil {
			return err
		}
		if ok {
			if setConfig(topicsAttr, ""); err != nil {
				return err
			}
		}
		eout.Info("cleared configuration: %s", topicsAttr)
		// Group
		groupAttr := spec + ".group"
		_, err, ok = getConfig(topicsAttr)
		if err != nil {
			return err
		}
		if ok {
			if setConfig(groupAttr, ""); err != nil {
				return err
			}
		}
		eout.Info("cleared configuration: %s", groupAttr)
	}
	return nil
}

func sourceConnectors() ([]string, error) {
	var specs []string
	rows, err := db.QueryContext(context.TODO(), "SELECT spec FROM connector WHERE spec LIKE 'src.%'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var spec string
		if err := rows.Scan(&spec); err != nil {
			return nil, err
		}
		specs = append(specs, spec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return specs, nil
}
