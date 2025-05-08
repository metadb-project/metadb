package libpq

import (
	"context"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/metadb-project/metadb/cmd/metadb/ast"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func registerUser(conn net.Conn, node *ast.RegisterUserStmt, db *dbx.DB, dc *pgx.Conn) error {
	exists, err := catalog.DatabaseUserExists(dc, node.UserName)
	if err != nil {
		return fmt.Errorf("selecting user: %w", err)
	}
	if !exists {
		return fmt.Errorf("user %q does not exist", node.UserName)
	}

	if err = register(dc, db.DBName, node.UserName); err != nil {
		return util.PGErr(err)
	}

	// Duplicated from CREATE USER, but necessary in case a DROP USER failed in midstream.
	if err = grantCreateOnUserSchema(dc, node.UserName); err != nil {
		_ = writeEncoded(conn, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "NOTICE",
			Message: util.PGErr(err).Error()},
		})
	}
	if err = grantUsageOnUserSchema(dc, node.UserName); err != nil {
		_ = writeEncoded(conn, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "NOTICE",
			Message: util.PGErr(err).Error()},
		})
	}

	return writeEncoded(conn, []pgproto3.Message{
		&pgproto3.CommandComplete{CommandTag: []byte("REGISTER USER")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func register(dc *pgx.Conn, dbname, user string) error {
	if _, err := dc.Exec(context.TODO(), "INSERT INTO metadb.auth (username) VALUES ($1) ON CONFLICT DO NOTHING", user); err != nil {
		return util.PGErr(err)
	}

	if _, err := dc.Exec(context.TODO(), "GRANT CREATE, CONNECT, TEMPORARY ON DATABASE "+dbname+" TO "+user); err != nil {
		return util.PGErr(err)
	}

	return nil
}
