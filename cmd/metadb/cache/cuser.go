package cache

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

type Users struct {
	users map[string]*util.RegexList
}

func NewUsers() (*Users, error) {
	// read users
	users, err := sysdb.UserRead()
	if err != nil {
		return nil, fmt.Errorf("reading user permissions: %s", err)
	}
	return &Users{users: users}, nil
}

func (u *Users) WithPerm(table *sqlx.Table) []string {
	var users []string
	for user, relist := range u.users {
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

func (u *Users) Get(username string) *util.RegexList {
	return u.users[username]
}

func (u *Users) Update(username, tables string) error {
	var regex []*regexp.Regexp
	str := strings.Split(tables, ",")
	for _, s := range str {
		re, err := regexp.Compile(s)
		if err != nil {
			return fmt.Errorf("invalid regular expression for user: %s: %s", username, s)
		}
		regex = append(regex, re)
	}
	u.users[username] = &util.RegexList{String: tables, Regex: regex}
	if err := sysdb.UserWrite(username, tables); err != nil {
		return fmt.Errorf("writing user permissions: %s: %s", username, tables)
	}
	return nil
}
