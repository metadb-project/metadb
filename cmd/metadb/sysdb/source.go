package sysdb

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
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
		"SELECT name, brokers, security, topics, consumergroup, schemapassfilter, schemaprefix FROM metadb.source")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var src = make([]*SourceConnector, 0)
	for rows.Next() {
		var name, brokers, security string
		var topics []string
		var consumergroup string
		var schemapassfilter []string
		var schemaprefix string
		if err := rows.Scan(&name, &brokers, &security, &topics, &consumergroup, &schemapassfilter, &schemaprefix); err != nil {
			return nil, err
		}
		if security == "" {
			security = "ssl"
		}
		src = append(src, &SourceConnector{
			Name:             name,
			Brokers:          brokers,
			Security:         security,
			Topics:           topics,
			Group:            consumergroup,
			SchemaPassFilter: schemapassfilter,
			SchemaPrefix:     schemaprefix,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return src, nil
}
