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
	"github.com/metadb-project/metadb/cmd/metadb/dberr"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func createUser(conn net.Conn /*query string,*/, node *ast.CreateUserStmt, db *dbx.DB, dc *pgx.Conn) error {
	if node.Options == nil {
		// return to client
	}
	opt, err := createUserOptions(node.Options)
	if err != nil {
		return err
	}
	if opt.Password == "" {
		return fmt.Errorf("option \"password\" is required")
	}

	exists, err := catalog.DatabaseUserExists(dc, node.UserName)
	if err != nil {
		return fmt.Errorf("selecting user: %w", err)
	}
	if exists {
		return fmt.Errorf("user %q already exists", node.UserName)
	}

	q := "CREATE USER " + node.UserName + " PASSWORD '" + opt.Password + "'"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}

	if opt.Comment != "" {
		if _, err = dc.Exec(context.TODO(), "COMMENT ON ROLE "+node.UserName+" IS '"+opt.Comment+"'"); err != nil {
			return err
		}
	}

	if err = register(dc, db.DBName, node.UserName); err != nil {
		return util.PGErr(err)
	}

	if err = createUserSchema(dc, node.UserName); err != nil {
		_ = writeEncoded(conn, []pgproto3.Message{&pgproto3.NoticeResponse{Severity: "NOTICE",
			Message: util.PGErr(err).Error()},
		})
	}
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
		&pgproto3.CommandComplete{CommandTag: []byte("CREATE USER")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	})
}

func createUserSchema(dc *pgx.Conn, user string) error {
	q := "CREATE SCHEMA " + user
	if _, err := dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	return nil
}

func grantCreateOnUserSchema(dc *pgx.Conn, user string) error {
	q := "GRANT CREATE ON SCHEMA " + user + " TO " + user
	if _, err := dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	return nil
}

func grantUsageOnUserSchema(dc *pgx.Conn, user string) error {
	q := "GRANT USAGE ON SCHEMA " + user + " TO " + user + " WITH GRANT OPTION"
	if _, err := dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	return nil
}

func createUserOptions(options []ast.Option) (*userOptions, error) {
	err := checkOptionDuplicates(options)
	if err != nil {
		return nil, err
	}
	o := &userOptions{
		// Set default values
	}
	for _, opt := range options {
		switch strings.ToLower(opt.Name) {
		case "password":
			o.Password = opt.Val
		case "comment":
			o.Comment = opt.Val
		default:
			return nil, &dberr.Error{
				Err:  fmt.Errorf("invalid option %q", opt.Name),
				Hint: "Valid options in this context are: password, comment",
			}
		}
	}
	return o, nil
}

type userOptions struct {
	Password string
	Comment  string
}
