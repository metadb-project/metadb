package sysdb

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func GoUpdateUserPerms(db *dbx.DB, trackedTables []dbx.Table) error {
	dc, err := db.Connect()
	if err != nil {
		return fmt.Errorf("updating user permissions: connecting to database: %v", err)
	}
	defer dbx.Close(dc)
	dcsuper, err := db.ConnectSuper()
	if err != nil {
		return fmt.Errorf("updating user permissions: connecting to database as superuser: %v", err)
	}
	defer dbx.Close(dcsuper)

	users, err := userRead(dc, true)
	if err != nil {
		return fmt.Errorf("updating user permissions: reading users: %v", err)
	}
	tables := make([]dbx.Table, len(trackedTables))
	copy(tables, trackedTables)
	for _, t := range trackedTables {
		tables = append(tables, t.Main())
	}
	batch := pgx.Batch{}
	for u, re := range users {
		for _, t := range tables {
			if re.String == "" {
				batch.Queue("REVOKE SELECT ON " + t.SQL() + " FROM " + u)
			} else {
				if util.UserPerm(re, &t) {
					batch.Queue("GRANT USAGE ON SCHEMA " + t.Schema + " TO " + u)
					batch.Queue("GRANT SELECT ON " + t.SQL() + " TO " + u)
				} else {
					batch.Queue("REVOKE SELECT ON " + t.SQL() + " FROM " + u)
				}
			}

		}
	}
	if err := dcsuper.SendBatch(context.TODO(), &batch).Close(); err != nil {
		return fmt.Errorf("updating user privileges: %w", err)
	}
	for u, re := range users {
		if re.String == "" {
			_, _ = dcsuper.Exec(context.TODO(), "REVOKE SELECT ON folio_source_record.marc__t FROM "+u)
			_, _ = dcsuper.Exec(context.TODO(), "REVOKE USAGE ON SCHEMA report FROM "+u)
			_, _ = dcsuper.Exec(context.TODO(), "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA report FROM "+u)
			_, _ = dcsuper.Exec(context.TODO(), "REVOKE USAGE ON SCHEMA folio_derived FROM "+u)
			_, _ = dcsuper.Exec(context.TODO(), "REVOKE SELECT ON ALL TABLES IN SCHEMA folio_derived FROM "+u)
			_, _ = dcsuper.Exec(context.TODO(), "REVOKE USAGE ON SCHEMA reshare_derived FROM "+u)
			_, _ = dcsuper.Exec(context.TODO(), "REVOKE SELECT ON ALL TABLES IN SCHEMA reshare_derived FROM "+u)
		} else {
			_, _ = dcsuper.Exec(context.TODO(), "GRANT SELECT ON folio_source_record.marc__t TO "+u)
			_, _ = dcsuper.Exec(context.TODO(), "GRANT USAGE ON SCHEMA report TO "+u)
			_, _ = dcsuper.Exec(context.TODO(), "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA report TO "+u)
			_, _ = dcsuper.Exec(context.TODO(), "GRANT USAGE ON SCHEMA folio_derived TO "+u)
			_, _ = dcsuper.Exec(context.TODO(), "GRANT SELECT ON ALL TABLES IN SCHEMA folio_derived TO "+u)
			_, _ = dcsuper.Exec(context.TODO(), "GRANT USAGE ON SCHEMA reshare_derived TO "+u)
			_, _ = dcsuper.Exec(context.TODO(), "GRANT SELECT ON ALL TABLES IN SCHEMA reshare_derived TO "+u)
		}
	}

	batch = pgx.Batch{}
	batch.Queue("UPDATE metadb.auth SET dbupdated=TRUE")
	batch.Queue("DELETE FROM metadb.auth WHERE tables=''")
	if err := dc.SendBatch(context.TODO(), &batch).Close(); err != nil {
		return fmt.Errorf("updating user authorizations: %w", err)
	}
	log.Trace("updated user permissions")
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

/*func UpdateUser(rq *api.UserUpdateRequest) error {
	sysMu.Lock()
	defer sysMu.Unlock()

	return userWrite(rq.Name, rq.Tables)
}
*/
/*func DeleteUser(rq *api.UserDeleteRequest) (*api.UserDeleteResponse, error) {
	sysMu.Lock()
	defer sysMu.Unlock()

	found, err := userDelete(rq.Name)
	if err != nil {
		return nil, err
	}
	return &api.UserDeleteResponse{NameNotFound: !found}, nil
}
*/
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
