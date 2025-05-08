package libpq

import (
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/acl"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func revokeAccessOnAll(conn net.Conn, node *ast.RevokeAccessOnAllStmt, dc *pgx.Conn) error {
	reg, err := catalog.UserRegistered(dc, node.UserName)
	if err != nil {
		return util.PGErr(err)
	}
	if !reg {
		return fmt.Errorf("%q is not a registered user", node.UserName)
	}

	if err := acl.RevokeAllFromUser(dc, node.UserName); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("REVOKE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func revokeAccessOnFunction(conn net.Conn, node *ast.RevokeAccessOnFunctionStmt, dc *pgx.Conn) error {
	reg, err := catalog.UserRegistered(dc, node.UserName)
	if err != nil {
		return util.PGErr(err)
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
	if err := acl.Revoke(dc, []acl.ACLItem{a}); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("REVOKE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func revokeAccessOnTable(conn net.Conn, node *ast.RevokeAccessOnTableStmt, dc *pgx.Conn) error {
	reg, err := catalog.UserRegistered(dc, node.UserName)
	if err != nil {
		return util.PGErr(err)
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
	if err := acl.Revoke(dc, []acl.ACLItem{a}); err != nil {
		return err
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("REVOKE")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}
