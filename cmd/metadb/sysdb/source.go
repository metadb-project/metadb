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
		"coalesce(consumergroup,''),coalesce(schemapassfilter,''),coalesce(schemastopfilter,''),"+
		"coalesce(tablestopfilter,''),coalesce(trimschemaprefix,''),coalesce(addschemaprefix,''),"+
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
		var consumergroup string
		var schemapassfilter string
		var schemastopfilter string
		var tablestopfilter string
		var trimschemaprefix string
		var addschemaprefix string
		var mapPublicSchema string
		var module string
		if err := rows.Scan(&name, &enable, &brokers, &security, &topics, &consumergroup, &schemapassfilter,
			&schemastopfilter, &tablestopfilter, &trimschemaprefix, &addschemaprefix, &mapPublicSchema,
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
			Group:            consumergroup,
			SchemaPassFilter: util.SplitList(schemapassfilter),
			SchemaStopFilter: util.SplitList(schemastopfilter),
			TableStopFilter:  util.SplitList(tablestopfilter),
			TrimSchemaPrefix: trimschemaprefix,
			AddSchemaPrefix:  addschemaprefix,
			MapPublicSchema:  mapPublicSchema,
			Module:           module,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return src, nil
}
