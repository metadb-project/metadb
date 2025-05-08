package libpq

import (
	"fmt"
	"net"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/acl"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func grantAccessOnAll(conn net.Conn, node *ast.GrantAccessOnAllStmt, dc *pgx.Conn) error {
	reg, err := catalog.UserRegistered(dc, node.UserName)
	if err != nil {
		return util.PGErr(err)
	}
	if !reg {
		return fmt.Errorf("%q is not a registered user", node.UserName)
	}

	acls := make([]acl.ACLItem, 0)

	syst := catalog.PublicSystemTables()
	for i := range syst {
		acls = append(acls, acl.ACLItem{
			SchemaName: syst[i].Schema,
			ObjectName: syst[i].Table,
			ObjectType: acl.Table,
			Privilege:  acl.Access,
			UserName:   node.UserName,
		})
	}

	tables, err := catalog.ReadDataTableNames(dc)
	if err != nil {
		return err
	}
	for i := range tables {
		acls = append(acls, acl.ACLItem{
			SchemaName: tables[i].Schema,
			ObjectName: tables[i].Table,
			ObjectType: acl.Table,
			Privilege:  acl.Access,
			UserName:   node.UserName,
		})
	}

	functions := catalog.FunctionNames()
	for i := range functions {
		acls = append(acls, acl.ACLItem{
			SchemaName: "public",
			ObjectName: functions[i],
			ObjectType: acl.Function,
			Privilege:  acl.Access,
			UserName:   node.UserName,
		})
	}

	for _, s := range extraManagedSchemas {
		var tables []string
		tables, err = catalog.ReadTablesInSchema(dc, s)
		if err != nil {
			return err
		}
		for i := range tables {
			acls = append(acls, acl.ACLItem{
				SchemaName: s,
				ObjectName: tables[i],
				ObjectType: acl.Table,
				Privilege:  acl.Access,
				UserName:   node.UserName,
			})
		}
	}
	for i := range extraManagedTables {
		t := strings.Split(extraManagedTables[i], ".")
		acls = append(acls, acl.ACLItem{
			SchemaName: t[0],
			ObjectName: t[1],
			ObjectType: acl.Table,
			Privilege:  acl.Access,
			UserName:   node.UserName,
		})
	}

	if err = acl.Grant(dc, acls); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("GRANT")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func grantAccessOnFunction(conn net.Conn, node *ast.GrantAccessOnFunctionStmt, dc *pgx.Conn) error {
	reg, err := catalog.UserRegistered(dc, node.UserName)
	if err != nil {
		return err
	}
	if !reg {
		return fmt.Errorf("%q is not a registered user", node.UserName)
	}

	function := strings.Split(node.FunctionName, ".")
	if len(function) < 2 {
		return fmt.Errorf("%q does not specify a schema and function", node.FunctionName)
	}
	functionSignature := function[1] + "(" + strings.Join(node.FunctionParameterTypes, ",") + ")"

	functionOK := catalog.IsFunction(functionSignature)
	if !functionOK {
		return fmt.Errorf("invalid function \"%s.%s\"", function[0], functionSignature)
	}

	a := acl.ACLItem{
		SchemaName: function[0],
		ObjectName: functionSignature,
		ObjectType: acl.Function,
		Privilege:  acl.Access,
		UserName:   node.UserName,
	}
	if err := acl.Grant(dc, []acl.ACLItem{a}); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("GRANT")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func grantAccessOnTable(conn net.Conn, node *ast.GrantAccessOnTableStmt, dc *pgx.Conn) error {
	reg, err := catalog.UserRegistered(dc, node.UserName)
	if err != nil {
		return err
	}
	if !reg {
		return fmt.Errorf("%q is not a registered user", node.UserName)
	}

	table := strings.Split(node.TableName, ".")
	if len(table) < 2 {
		return fmt.Errorf("%q does not specify a schema and table", node.TableName)
	}

	tableOK, err := isManagedTable(dc, table[0], table[1])
	if err != nil {
		return err
	}
	if !tableOK {
		return fmt.Errorf("invalid table %q", node.TableName)
	}

	a := acl.ACLItem{
		SchemaName: table[0],
		ObjectName: table[1],
		ObjectType: acl.Table,
		Privilege:  acl.Access,
		UserName:   node.UserName,
	}
	if err := acl.Grant(dc, []acl.ACLItem{a}); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("GRANT")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func isManagedTable(dq dbx.Queryable, schema, table string) (bool, error) {
	var ok bool

	isSystemTable := catalog.IsPublicSystemTable(schema, table)
	if isSystemTable {
		ok = true
	}

	isDataTable, err := catalog.IsDataTable(dq, schema, strings.TrimSuffix(table, "__"))
	if err != nil {
		return false, err
	}
	if isDataTable {
		ok = true
	}

	if slices.Contains(extraManagedSchemas, schema) || slices.Contains(extraManagedTables, schema+"."+table) {
		ok = true
	}
	return ok, nil
}
