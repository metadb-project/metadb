package cat

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func AllUsers(dc *pgx.Conn) ([]string, error) {
	q := "SELECT username FROM metadb.auth"
	rows, err := dc.Query(context.TODO(), q)
	if err != nil {
		return nil, fmt.Errorf("selecting user list: %v", err)
	}
	defer rows.Close()
	users := make([]string, 0)
	for rows.Next() {
		var username string
		err := rows.Scan(&username)
		if err != nil {
			return nil, fmt.Errorf("reading user list: %v", err)
		}
		users = append(users, username)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading user list: %v", err)
	}
	return users, nil
}
