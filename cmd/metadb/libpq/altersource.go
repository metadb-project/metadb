package libpq

import (
	"context"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/dberr"
)

func alterDataSource(conn net.Conn, node *ast.AlterDataSourceStmt, dc *pgx.Conn) error {
	exists, err := sourceExists(dc, node.DataSourceName)
	if err != nil {
		return fmt.Errorf("selecting data source: %w", err)
	}
	if !exists {
		return fmt.Errorf("data source %q does not exist", node.DataSourceName)
	}

	if err = alterSourceOptions(dc, node); err != nil {
		return err
	}

	_ = writeEncoded(conn, []pgproto3.Message{
		&pgproto3.NoticeResponse{Severity: "INFO", Message: "restart server for data source changes to take effect"},
	})

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("ALTER DATA SOURCE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func alterSourceOptions(dc *pgx.Conn, node *ast.AlterDataSourceStmt) error {
	for _, opt := range node.Options {
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
			fallthrough
		case "security":
			fallthrough
		case "topics":
			fallthrough
		case "consumer_group":
			fallthrough
		case "schema_pass_filter":
			fallthrough
		case "schema_stop_filter":
			fallthrough
		case "table_stop_filter":
			fallthrough
		case "trim_schema_prefix":
			fallthrough
		case "add_schema_prefix":
			fallthrough
		case "map_public_schema":
			fallthrough
		case "module":
			// NOP
		default:
			return &dberr.Error{
				Err: fmt.Errorf("invalid option %q", opt.Name),
				Hint: "Valid options in this context are: " +
					"brokers, security, topics, consumer_group, schema_pass_filter, schema_stop_filter, table_stop_filter, trim_schema_prefix, add_schema_prefix, map_public_schema, module",
			}
		}
		isnull, err := isSourceOptionNull(dc, node.DataSourceName, name)
		if err != nil {
			return fmt.Errorf("reading source option: %w", err)
		}
		if opt.Action == "DROP" {
			if isnull {
				return fmt.Errorf("option %q not found", name)
			}
			err := updateSource(dc, node.DataSourceName, name, "NULL")
			if err != nil {
				return fmt.Errorf("unable to drop option %q", name)
			}
		}
		if opt.Action == "SET" {
			if isnull {
				return fmt.Errorf("option %q not found", name)
			}
			err := updateSource(dc, node.DataSourceName, name, "'"+opt.Val+"'")
			if err != nil {
				return fmt.Errorf("unable to set option %q", name)
			}
		}
		if opt.Action == "ADD" {
			if !isnull {
				return fmt.Errorf("option %q provided more than once", name)
			}
			err := updateSource(dc, node.DataSourceName, name, "'"+opt.Val+"'")
			if err != nil {
				return fmt.Errorf("unable to add option %q", name)
			}
		}
	}

	return nil
}

func isSourceOptionNull(dc *pgx.Conn, sourceName, optionName string) (bool, error) {
	var val *string
	q := "SELECT " + optionName + " FROM metadb.source WHERE name='" + sourceName + "'"
	err := dc.QueryRow(context.TODO(), q).Scan(&val)
	switch {
	case err == pgx.ErrNoRows:
		return false, fmt.Errorf("data source %q does not exist", sourceName)
	case err != nil:
		return false, fmt.Errorf("reading data source: %w", err)
	default:
		return val == nil, nil
	}
}
