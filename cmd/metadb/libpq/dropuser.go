package libpq

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func dropUser(conn net.Conn, node *ast.DropUserStmt, db *dbx.DB, dc *pgx.Conn) error {
	exists, err := catalog.DatabaseUserExists(dc, node.UserName)
	if err != nil {
		return fmt.Errorf("selecting user: %w", err)
	}
	if !exists {
		return fmt.Errorf("user %q does not exist", node.UserName)
	}

	dcsuper, err := db.ConnectSuper()
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer dbx.Close(dcsuper)

	if err = deregister(db, dc, dcsuper, node.UserName, db.DBName); err != nil {
		return err
	}

	q := "DROP USER " + node.UserName
	if _, err = dcsuper.Exec(context.TODO(), q); err != nil {
		return util.PGErr(err)
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("DROP USER")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func functionsOwnedByUser(dq dbx.Queryable, user string) ([]dbx.Function, error) {
	rows, err := dq.Query(context.TODO(),
		"SELECT n.nspname, p.proname "+
			"FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid JOIN pg_roles r ON p.proowner=r.oid "+
			"WHERE r.rolname=$1",
		user)
	if err != nil {
		return nil, fmt.Errorf("selecting functions: %w", util.PGErr(err))
	}
	defer rows.Close()
	functions := make([]dbx.Function, 0)
	for rows.Next() {
		var s, f string
		if err = rows.Scan(&s, &f); err != nil {
			return nil, fmt.Errorf("reading functions: %w", util.PGErr(err))
		}
		functions = append(functions, dbx.Function{Schema: s, Function: f})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("reading functions: %w", util.PGErr(err))
	}
	return functions, nil
}

func tablesOwnedByUser(dq dbx.Queryable, user string) ([]dbx.Table, error) {
	rows, err := dq.Query(context.TODO(),
		"SELECT schemaname, tablename FROM pg_tables WHERE tableowner=$1",
		user)
	if err != nil {
		return nil, fmt.Errorf("selecting tables: %w", util.PGErr(err))
	}
	defer rows.Close()
	tables := make([]dbx.Table, 0)
	for rows.Next() {
		var s, t string
		if err = rows.Scan(&s, &t); err != nil {
			return nil, fmt.Errorf("reading tables: %w", util.PGErr(err))
		}
		tables = append(tables, dbx.Table{Schema: s, Table: t})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("reading tables: %w", util.PGErr(err))
	}
	return tables, nil
}

func schemasWithUserPrivileges(dq dbx.Queryable, user string) ([]string, error) {
	rows, err := dq.Query(context.TODO(),
		"SELECT nspname, nspacl FROM pg_namespace")
	if err != nil {
		return nil, fmt.Errorf("selecting schemas: %w", util.PGErr(err))
	}
	defer rows.Close()
	schemas := make([]string, 0)
	prefix := user + "="
	for rows.Next() {
		var schema string
		var aclItems []string
		if err = rows.Scan(&schema, &aclItems); err != nil {
			return nil, fmt.Errorf("reading schemas: %w", util.PGErr(err))
		}
		for i := range aclItems {
			if strings.HasPrefix(aclItems[i], prefix) {
				schemas = append(schemas, schema)
				break
			}
		}
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("reading schemas: %w", util.PGErr(err))
	}
	return schemas, nil
}
