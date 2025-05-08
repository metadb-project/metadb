package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func UserRegistered(dq dbx.Queryable, user string) (bool, error) {
	q := "SELECT 1 FROM metadb.auth WHERE username=$1"
	var i int64
	err := dq.QueryRow(context.TODO(), q, user).Scan(&i)
	switch {
	case err == pgx.ErrNoRows:
		return false, nil
	case err != nil:
		return false, util.PGErr(err)
	default:
		return true, nil
	}
}

func Users(dq dbx.Queryable) ([]string, error) {
	q := "SELECT username FROM metadb.auth"
	rows, err := dq.Query(context.TODO(), q)
	if err != nil {
		return nil, fmt.Errorf("selecting user list: %w", util.PGErr(err))
	}
	defer rows.Close()
	users := make([]string, 0)
	for rows.Next() {
		var u string
		err := rows.Scan(&u)
		if err != nil {
			return nil, fmt.Errorf("reading user list: %w", util.PGErr(err))
		}
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading user list: %w", util.PGErr(err))
	}
	return users, nil
}

func DatabaseUserExists(dq dbx.Queryable, user string) (bool, error) {
	q := "SELECT 1 FROM pg_catalog.pg_user WHERE usename=$1"
	var i int64
	err := dq.QueryRow(context.TODO(), q, user).Scan(&i)
	switch {
	case err == pgx.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}
