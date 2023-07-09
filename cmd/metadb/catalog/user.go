package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

// TODO Change to instance method and use cache
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

func (c *Catalog) initUsers() error {
	// read users
	users, err := sysdb.UserRead(c.dp)
	if err != nil {
		return fmt.Errorf("reading user permissions: %v", err)
	}
	c.users = users
	return nil
}

//func (c *Catalog) UsersWithPerm(table *sqlx.Table) []string {
//	c.mu.Lock()
//	defer c.mu.Unlock()
//	return usersWithPerm(c, table)
//}

func usersWithPerm(cat *Catalog, table *dbx.Table) []string {
	var users []string
	for user, relist := range cat.users {
		if util.UserPerm(relist, table) {
			users = append(users, user)
		}
	}
	return users
}

//func (u *Users) Perm(username string, schema, table string) bool {
//        reList := u.Get(username)
//        if reList == nil {
//                return false
//        }
//        return UserPerm(reList, schema, table)
//}

func (c *Catalog) GetUserRegexList(username string) *util.RegexList {
	c.mu.Lock()
	defer c.mu.Unlock()
	rl := c.users[username]
	return rl
}

/*// func (c *Catalog) Update(username, tables string) error {
func (c *Catalog) UpdateUser(username, tables string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var regex []*regexp.Regexp
	str := strings.Split(tables, ",")
	for _, s := range str {
		re, err := regexp.Compile(s)
		if err != nil {
			return fmt.Errorf("invalid regular expression for user: %s: %s", username, s)
		}
		regex = append(regex, re)
	}
	c.users[username] = &util.RegexList{String: tables, Regex: regex}
	if err := sysdb.UserWrite(username, tables); err != nil {
		return fmt.Errorf("writing user permissions: %s: %s", username, tables)
	}
	return nil
}*/
