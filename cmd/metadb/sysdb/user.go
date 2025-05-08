package sysdb

import (
	"context"
	"regexp"
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func UserRead(dq dbx.Queryable) (map[string]*util.RegexList, error) {
	return userRead(dq, false)
}

func userRead(dq dbx.Queryable, notUpdatedOnly bool) (map[string]*util.RegexList, error) {
	users := make(map[string]*util.RegexList)
	q := "SELECT username, tables, dbupdated FROM metadb.auth"
	rows, err := dq.Query(context.TODO(), q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

/*func UserWrite(username, tables string) error {
	return userWrite(username, tables)
}
*/

/*func userWrite(username, tables string) error {
	q := "INSERT INTO userperm(username,tables,dbupdated) VALUES ('" + username + "','" + tables + "',FALSE) ON CONFLICT (username) DO UPDATE SET tables='" + tables + "',dbupdated=FALSE;"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("insert: %v: %s", username, err)
	}
	return nil
}
*/

/*func userDelete(username string) (bool, error) {
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
*/
