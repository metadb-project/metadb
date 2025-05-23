package sysdb

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func ReadSourceConnectors(db *dbx.DB) ([]*SourceConnector, error) {
	var dbc *pgx.Conn
	var err error
	if dbc, err = db.Connect(); err != nil {
		return nil, err
	}
	defer dbc.Close(context.TODO())

	var rows pgx.Rows
	rows, err = dbc.Query(context.TODO(), ""+
		"SELECT name,enable,coalesce(brokers,''),coalesce(security,''),coalesce(topics,''),"+
		"coalesce(consumer_group,''),coalesce(schema_pass_filter,''),coalesce(schema_stop_filter,''),"+
		"coalesce(table_stop_filter,''),coalesce(trim_schema_prefix,''),coalesce(add_schema_prefix,''),"+
		"coalesce(map_public_schema,''),coalesce(module,'') FROM metadb.source")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var src = make([]*SourceConnector, 0)
	for rows.Next() {
		var name, brokers, security string
		var enable bool
		var topics string
		var consumerGroup string
		var schemaPassFilter string
		var schemaStopFilter string
		var tableStopFilter string
		var trimSchemaPrefix string
		var addSchemaPrefix string
		var mapPublicSchema string
		var module string
		if err := rows.Scan(&name, &enable, &brokers, &security, &topics, &consumerGroup, &schemaPassFilter,
			&schemaStopFilter, &tableStopFilter, &trimSchemaPrefix, &addSchemaPrefix, &mapPublicSchema,
			&module); err != nil {
			return nil, err
		}
		if security == "" {
			security = "ssl"
		}
		src = append(src, &SourceConnector{
			Name:             name,
			Enable:           enable,
			Brokers:          brokers,
			Security:         security,
			Topics:           strings.Split(topics, ","),
			Group:            consumerGroup,
			SchemaPassFilter: util.SplitList(schemaPassFilter),
			SchemaStopFilter: util.SplitList(schemaStopFilter),
			TableStopFilter:  util.SplitList(tableStopFilter),
			TrimSchemaPrefix: trimSchemaPrefix,
			AddSchemaPrefix:  addSchemaPrefix,
			MapPublicSchema:  mapPublicSchema,
			Module:           module,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return src, nil
}
