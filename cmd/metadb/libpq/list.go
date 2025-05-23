package libpq

import (
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
)

func list(conn net.Conn, node *ast.ListStmt, dc *pgx.Conn, sources *[]*sysdb.SourceConnector) error {
	switch strings.ToLower(node.Name) {
	case "authorizations":
		return proxySelect(conn, ""+
			"SELECT username,"+
			"       CASE WHEN (NOT dbupdated) THEN 'pending restart'"+
			"            WHEN (tables='.*' AND dbupdated) THEN 'authorized'"+
			"            ELSE 'not authorized'"+
			"       END note"+
			"    FROM metadb.auth"+
			"    ORDER BY username", nil, dc)
	case "config":
		return proxySelect(conn, ""+
			"SELECT parameter, value FROM metadb.config ORDER BY parameter", nil, dc)
	case "data_mappings":
		return proxySelect(conn, ""+
			"SELECT 'json' mapping_type,"+
			"       schema_name||'.'||table_name||'__' table_name,"+
			"       column_name,"+
			"       path object_path,"+
			"       map target_identifier"+
			"    FROM metadb.transform_json"+
			"    ORDER BY mapping_type, table_name, column_name, path", nil, dc)
	case "data_origins":
		return proxySelect(conn, ""+
			"SELECT name"+
			"    FROM metadb.origin"+
			"    ORDER BY name", nil, dc)
	case "data_sources":
		return proxySelect(conn, ""+
			"SELECT name,"+
			"       brokers,"+
			"       security,"+
			"       topics,"+
			"       consumer_group,"+
			"       schema_pass_filter,"+
			"       schema_stop_filter,"+
			"       table_stop_filter,"+
			"       trim_schema_prefix,"+
			"       add_schema_prefix,"+
			"       map_public_schema,"+
			"       module"+
			"    FROM metadb.source"+
			"    ORDER BY name", nil, dc)
	case "status":
		return listStatus(conn, sources)
	default:
		return fmt.Errorf("unrecognized parameter %q", node.Name)
	}
}

func listStatus(conn net.Conn, sources *[]*sysdb.SourceConnector) error {
	m := []pgproto3.Message{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("type"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
			{
				Name:                 []byte("name"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
			{
				Name:                 []byte("source_stream"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
			{
				Name:                 []byte("source_sync"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		}},
	}
	for _, s := range *sources {
		m = append(m, &pgproto3.DataRow{Values: [][]byte{
			[]byte("data_source"),
			[]byte(s.Name),
			[]byte(s.Status.Stream.GetString()),
			[]byte(s.Status.Sync.GetString()),
		}})
	}
	ctag := fmt.Sprintf("SELECT %d", len(*sources))
	m = append(m, &pgproto3.CommandComplete{CommandTag: []byte(ctag)})
	m = append(m, &pgproto3.ReadyForQuery{TxStatus: 'I'})
	return writeEncoded(conn, m)
}
