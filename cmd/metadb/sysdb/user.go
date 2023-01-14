package sysdb

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func UpdateUserPerms(adb sqlx.DB, tables []dbx.Table) error {
	users, err := userRead(adb, true)
	if err != nil {
		return err
	}
	for u, re := range users {
		for _, oldt := range tables {
			t := sqlx.Table{Schema: oldt.S, Table: oldt.T}
			if re.String == "" {
				// Revoke
				_, _ = adb.Exec(nil, "REVOKE USAGE ON SCHEMA "+adb.IdentiferSQL(t.Schema)+" FROM "+u)
				_, _ = adb.Exec(nil, "REVOKE SELECT ON "+adb.TableSQL(&t)+" FROM "+u)
				_, _ = adb.Exec(nil, "REVOKE SELECT ON "+adb.HistoryTableSQL(&t)+" FROM "+u)
			} else {
				// Grant if regex matches
				if util.UserPerm(re, &t) {
					_, _ = adb.Exec(nil, "GRANT USAGE ON SCHEMA "+adb.IdentiferSQL(t.Schema)+" TO "+u)
					_, _ = adb.Exec(nil, "GRANT SELECT ON "+adb.TableSQL(&t)+" TO "+u)
					_, _ = adb.Exec(nil, "GRANT SELECT ON "+adb.HistoryTableSQL(&t)+" TO "+u)
				} else {
					_, _ = adb.Exec(nil, "REVOKE SELECT ON "+adb.TableSQL(&t)+" FROM "+u)
					_, _ = adb.Exec(nil, "REVOKE SELECT ON "+adb.HistoryTableSQL(&t)+" FROM "+u)
				}
			}

		}
	}
	if _, err := adb.Exec(nil, "UPDATE metadb.auth SET dbupdated=TRUE"); err != nil {
		return fmt.Errorf("sysdb: update: %s", err)
	}
	if _, err := adb.Exec(nil, "DELETE FROM metadb.auth WHERE tables=''"); err != nil {
		return fmt.Errorf("sysdb: update: %s", err)
	}
	return nil
}

/*
func ListUser(rq *api.UserListRequest) (*api.UserListResponse, error) {
	sysMu.Lock()
	defer sysMu.Unlock()

	// read users
	var rs = &api.UserListResponse{}
	users, err := userRead(false)
	if err != nil {
		return nil, err
	}
	for u, re := range users {
		if rq.Name != "" && rq.Name != u {
			continue
		}
		rs.Users = append(rs.Users, api.UserItem{Name: u, Tables: re.String})
	}
	sort.Slice(rs.Users, func(i, j int) bool {
		return rs.Users[i].Name < rs.Users[j].Name
	})
	return rs, nil
}
*/

func UpdateUser(rq *api.UserUpdateRequest) error {
	sysMu.Lock()
	defer sysMu.Unlock()

	return userWrite(rq.Name, rq.Tables)
}

func DeleteUser(rq *api.UserDeleteRequest) (*api.UserDeleteResponse, error) {
	sysMu.Lock()
	defer sysMu.Unlock()

	found, err := userDelete(rq.Name)
	if err != nil {
		return nil, err
	}
	return &api.UserDeleteResponse{NameNotFound: !found}, nil
}

func UserRead(db sqlx.DB) (map[string]*util.RegexList, error) {
	return userRead(db, false)
}

func userRead(adb sqlx.DB, notUpdatedOnly bool) (map[string]*util.RegexList, error) {
	users := make(map[string]*util.RegexList)
	q := "SELECT username, tables, dbupdated FROM metadb.auth"
	rows, err := adb.Query(nil, q)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	for rows.Next() {
		var username, tables string
		var dbupdated bool
		if err := rows.Scan(&username, &tables, &dbupdated); err != nil {
			return nil, err
		}
		if notUpdatedOnly && dbupdated {
			continue
		}
		var regex []*regexp.Regexp
		str := strings.Split(tables, ",")
		for _, s := range str {
			re, err := regexp.Compile("\\b" + s + "\\b")
			if err != nil {
				log.Error("sysdb: invalid regular expression for user: %s: %s", username, s)
				continue
			}
			regex = append(regex, re)
		}
		users[username] = &util.RegexList{String: tables, Regex: regex}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return users, nil
}

func UserWrite(username, tables string) error {
	sysMu.Lock()
	defer sysMu.Unlock()

	return userWrite(username, tables)
}

func userWrite(username, tables string) error {
	q := "INSERT INTO userperm(username,tables,dbupdated) VALUES ('" + username + "','" + tables + "',FALSE) ON CONFLICT (username) DO UPDATE SET tables='" + tables + "',dbupdated=FALSE;"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("insert: %v: %s", username, err)
	}
	return nil
}

func userDelete(username string) (bool, error) {
	result, err := db.ExecContext(context.TODO(), "UPDATE userperm SET tables='',dbupdated=FALSE WHERE username='"+username+"'")
	if err != nil {
		return false, fmt.Errorf("delete: %s: %s", username, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete: %s: %s", username, err)
	}
	return rows != 0, nil
}
