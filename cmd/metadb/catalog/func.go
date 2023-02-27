package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

var functionDefs = [][]string{
	{"mdbversion()", `
CREATE FUNCTION public.mdbversion() RETURNS text
    AS $$ SELECT 'Metadb ` + util.MetadbVersion + `' $$
    LANGUAGE SQL`},
	{"ps()", `
CREATE FUNCTION public.ps() RETURNS TABLE(dbname text, username text, state text, realtime text, query text)
    AS $$
       SELECT datname::text dbname, usename::text username, state, to_char(now() - query_start, 'HH24:MI:SS') AS realtime, query
           FROM pg_stat_activity
           WHERE leader_pid IS NULL AND pid <> pg_backend_pid() AND state <> 'idle'
           ORDER BY query_start
       $$
    LANGUAGE SQL`},
	{"mdblog(interval)", `
CREATE FUNCTION public.mdblog(v interval default interval '24 hours') RETURNS TABLE(log_time timestamp(3) with time zone, error_severity text, message text)
    AS $$
       SELECT log_time, error_severity, message
           FROM metadb.log
           WHERE log_time >= now() - v
           ORDER BY log_time
       $$
    LANGUAGE SQL`},
}

func CreateAllFunctions(dcsuper, dc *pgx.Conn, systemuser string) error {
	q := "GRANT CREATE, USAGE ON SCHEMA public TO " + systemuser
	_, err := dcsuper.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("granting systemuser access to public schema: %v", err)
	}

	users, err := AllUsers(dc)
	if err != nil {
		return fmt.Errorf("accessing user list: %v", err)
	}

	for _, f := range functionDefs {
		err := createFunction(dc, f[0], f[1], users)
		if err != nil {
			return fmt.Errorf("creating %q: %v", f[0], err)
		}
	}

	for _, u := range users {
		q := "GRANT USAGE ON SCHEMA public TO " + u
		_, _ = dcsuper.Exec(context.TODO(), q)
	}

	log.Trace("created database functions")
	return nil
}

func createFunction(dc *pgx.Conn, fname, fdef string, users []string) error {
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return fmt.Errorf("starting transaction for function: %v", err)
	}
	defer dbx.Rollback(tx)

	q := "DROP FUNCTION IF EXISTS public." + fname
	_, err = tx.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("dropping function: %v", err)
	}

	_, err = tx.Exec(context.TODO(), fdef)
	if err != nil {
		return fmt.Errorf("creating function: %v", err)
	}

	err = tx.Commit(context.TODO())
	if err != nil {
		return err
	}

	for _, u := range users {
		q := "GRANT EXECUTE ON FUNCTION public." + fname + " TO " + u
		_, _ = tx.Exec(context.TODO(), q)
	}

	return nil
}
