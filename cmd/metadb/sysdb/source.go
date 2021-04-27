package sysdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/metadb/database"
)

func ReadSourceConnectors() ([]*SourceConnector, error) {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	var sc []*SourceConnector
	if sc, err = readSources(); err != nil {
		return nil, err
	}
	var x int
	var c *SourceConnector
	for x, c = range sc {
		if err = readTopics(c.ID, c); err != nil {
			return nil, err
		}
		if err = readSchemaPassFilter(c.ID, c); err != nil {
			return nil, err
		}
		sc[x] = c
	}
	return sc, nil
}

func readSources() ([]*SourceConnector, error) {
	var rows *sql.Rows
	var err error
	var s = "" +
		"SELECT id, name, brokers, group_id, schema_prefix\n" +
		"    FROM connect_source_kafka\n" +
		"    ORDER BY name;"
	if rows, err = db.QueryContext(context.TODO(), s); err != nil {
		return nil, err
	}
	defer rows.Close()
	var sc []*SourceConnector
	for rows.Next() {
		var c SourceConnector
		if err = rows.Scan(&c.ID, &c.Name, &c.Brokers, &c.Group, &c.SchemaPrefix); err != nil {
			return nil, err
		}
		sc = append(sc, &c)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return sc, nil
}

func readTopics(source_id int64, c *SourceConnector) error {
	var rows *sql.Rows
	var err error
	var s = fmt.Sprintf(""+
		"SELECT topic\n"+
		"    FROM connect_source_kafka_topic\n"+
		"    WHERE source_id = %d\n"+
		"    ORDER BY id;", source_id)
	if rows, err = db.QueryContext(context.TODO(), s); err != nil {
		return err
	}
	defer rows.Close()
	var topics []string
	for rows.Next() {
		var t string
		if err = rows.Scan(&t); err != nil {
			return err
		}
		topics = append(topics, t)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	c.Topics = topics
	return nil
}

func readSchemaPassFilter(source_id int64, c *SourceConnector) error {
	var rows *sql.Rows
	var err error
	var s = fmt.Sprintf(""+
		"SELECT schema_pass_filter\n"+
		"    FROM connect_source_kafka_schema_pass_filter\n"+
		"    WHERE source_id = %d\n"+
		"    ORDER BY id;", source_id)
	if rows, err = db.QueryContext(context.TODO(), s); err != nil {
		return err
	}
	defer rows.Close()
	var filter []string
	for rows.Next() {
		var f string
		if err = rows.Scan(&f); err != nil {
			return err
		}
		filter = append(filter, f)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	c.SchemaPassFilter = filter
	return nil
}

func UpdateKafkaSource(rq api.UpdateSourceConnectorRequest) error {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	// Check if source name already exists.
	var s = fmt.Sprintf("SELECT id FROM connect_source_kafka WHERE name = '%s';", rq.Name)
	var id int64
	err = db.QueryRowContext(context.TODO(), s).Scan(&id)
	switch {
	case err == sql.ErrNoRows:
		// NOP
	case err != nil:
		return err
	default:
		return fmt.Errorf("modifying a source connector not yet supported")
	}

	// More than one source not currently supported.
	s = fmt.Sprintf("SELECT count(*) FROM connect_source_kafka;")
	var count int64
	if err = db.QueryRowContext(context.TODO(), s).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("more than one source connector not currently supported")
	}

	var databaseConnectors []*DatabaseConnector
	if databaseConnectors, err = readDatabases(); err != nil {
		return err
	}

	var tx *sql.Tx
	if tx, err = database.MakeTx(db); err != nil {
		return err
	}
	defer tx.Rollback()

	s = fmt.Sprintf(""+
		"INSERT INTO connect_source_kafka (\n"+
		"    name, brokers, group_id, schema_prefix\n"+
		") VALUES (\n"+
		"    '%s', '%s', '%s', '%s'\n"+
		");", rq.Name, rq.Config.Brokers, rq.Config.Group, rq.Config.SchemaPrefix,
	)
	var result sql.Result
	if result, err = tx.ExecContext(context.TODO(), s); err != nil {
		return fmt.Errorf("writing source: %s: %s", err, s)
	}
	var sourceId int64
	if sourceId, err = result.LastInsertId(); err != nil {
		return err
	}

	var t string
	for _, t = range rq.Config.Topics {
		s = fmt.Sprintf(""+
			"INSERT INTO connect_source_kafka_topic (\n"+
			"    source_id, topic\n"+
			") VALUES (\n"+
			"    %d, '%s'\n"+
			");", sourceId, t)
		if _, err = tx.ExecContext(context.TODO(), s); err != nil {
			return err
		}
	}

	var f string
	for _, f = range rq.Config.SchemaPassFilter {
		s = fmt.Sprintf(""+
			"INSERT INTO connect_source_kafka_schema_pass_filter (\n"+
			"    source_id, schema_pass_filter\n"+
			") VALUES (\n"+
			"    %d, '%s'\n"+
			");", sourceId, f)
		if _, err = tx.ExecContext(context.TODO(), s); err != nil {
			return err
		}
	}

	var rqDatabaseName string
	for _, rqDatabaseName = range rq.Config.Databases {
		var databaseID int64
		var dc *DatabaseConnector
		for _, dc = range databaseConnectors {
			if dc.Name == rqDatabaseName {
				databaseID = dc.ID
				break
			}
		}
		if databaseID == 0 {
			return fmt.Errorf("database connector \"%s\" not found", rqDatabaseName)
		}
		s = fmt.Sprintf(""+
			"INSERT INTO connect_source_kafka_database (\n"+
			"    source_id, database_id\n"+
			") VALUES (\n"+
			"    %d, %d\n"+
			");", sourceId, databaseID)
		if _, err = tx.ExecContext(context.TODO(), s); err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("writing kafka source: committing changes: %s", err)
	}

	return nil
}
