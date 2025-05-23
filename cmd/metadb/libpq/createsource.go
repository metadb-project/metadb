package libpq

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/dberr"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
)

func createDataSource(conn net.Conn, node *ast.CreateDataSourceStmt, dc *pgx.Conn) error {
	exists, err := sourceExists(dc, node.DataSourceName)
	if err != nil {
		return fmt.Errorf("selecting data source: %w", err)
	}
	if exists {
		return fmt.Errorf("data source %q already exists", node.DataSourceName)
	}

	var count int64
	q := "SELECT count(*) FROM metadb.source"
	err = dc.QueryRow(context.TODO(), q).Scan(&count)
	if err != nil {
		return fmt.Errorf("checking number of configured sources: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("multiple sources not currently supported")
	}

	name := node.DataSourceName
	if node.TypeName != "kafka" {
		return fmt.Errorf("invalid data source type %q", node.TypeName)
	}
	if node.Options == nil {
		// return to client
	}
	src, err := createSourceOptions(node.Options)
	if err != nil {
		return err
	}

	q = "INSERT INTO metadb.source" +
		"(name,brokers,security,topics,consumer_group,schema_pass_filter,schema_stop_filter,table_stop_filter,trim_schema_prefix,add_schema_prefix,map_public_schema,module,enable)" +
		"VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"
	_, err = dc.Exec(context.TODO(), q,
		name, src.Brokers, src.Security, strings.Join(src.Topics, ","), src.Group,
		strings.Join(src.SchemaPassFilter, ","), strings.Join(src.SchemaStopFilter, ","),
		strings.Join(src.TableStopFilter, ","), src.TrimSchemaPrefix, src.AddSchemaPrefix,
		src.MapPublicSchema, src.Module, src.Enable)
	if err != nil {
		return fmt.Errorf("writing source configuration: %w", err)
	}
	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("CREATE DATA SOURCE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func createSourceOptions(options []ast.Option) (*sysdb.SourceConnector, error) {
	err := checkOptionDuplicates(options)
	if err != nil {
		return nil, err
	}
	s := &sysdb.SourceConnector{
		// Set default values
		Enable:           true,
		Security:         "ssl",
		Topics:           []string{},
		SchemaPassFilter: []string{},
		SchemaStopFilter: []string{},
		TableStopFilter:  []string{},
	}
	for _, opt := range options {
		var name string
		switch opt.Name {
		case "consumergroup":
			name = "consumer_group"
		case "schemapassfilter":
			name = "schema_pass_filter"
		case "schemastopfilter":
			name = "schema_stop_filter"
		case "tablestopfilter":
			name = "table_stop_filter"
		case "trimschemaprefix":
			name = "trim_schema_prefix"
		case "addschemaprefix":
			name = "add_schema_prefix"
		default:
			name = opt.Name
		}
		switch name {
		case "brokers":
			s.Brokers = opt.Val
		case "security":
			s.Security = opt.Val
		case "topics":
			s.Topics = strings.Split(opt.Val, ",")
		case "consumer_group":
			s.Group = opt.Val
		case "schema_pass_filter":
			s.SchemaPassFilter = strings.Split(opt.Val, ",")
		case "schema_stop_filter":
			s.SchemaStopFilter = strings.Split(opt.Val, ",")
		case "table_stop_filter":
			s.TableStopFilter = strings.Split(opt.Val, ",")
		case "trim_schema_prefix":
			s.TrimSchemaPrefix = opt.Val
		case "add_schema_prefix":
			s.AddSchemaPrefix = opt.Val
		case "map_public_schema":
			s.MapPublicSchema = opt.Val
		//case "enable":
		//	s.Enable = (strings.ToLower(opt.Val) == "true")
		case "module":
			s.Module = opt.Val
		default:
			return nil, &dberr.Error{
				Err: fmt.Errorf("invalid option %q", opt.Name),
				Hint: "Valid options in this context are: " +
					"brokers, security, topics, consumer_group, schema_pass_filter, schema_stop_filter, table_stop_filter, trim_schema_prefix, add_schema_prefix, map_public_schema, module",
			}
		}
	}
	return s, nil
}
