package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/acl"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

var functionDefs = [][]string{
	{"mdbversion()", `
CREATE FUNCTION public.mdbversion() RETURNS text
    AS $$ SELECT 'Metadb ` + util.MetadbVersion + `' $$
    LANGUAGE SQL`},
	{"mdbusers()", `
    CREATE FUNCTION public.mdbusers() RETURNS TABLE(username text)
    AS $$
       SELECT username FROM metadb.auth ORDER BY username
       $$
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

func FunctionNames() []string {
	f := make([]string, 0)
	for i := range functionDefs {
		f = append(f, functionDefs[i][0])
	}
	return f
}

func IsFunction(functionSignature string) bool {
	for i := range functionDefs {
		if functionDefs[i][0] == functionSignature {
			return true
		}
	}
	return false
}

func CreateAllFunctions(dcsuper, dc *pgx.Conn) error {
	for i := range functionDefs {
		err := createFunction(dc, functionDefs[i][0], functionDefs[i][1])
		if err != nil {
			return fmt.Errorf("creating %q: %v", functionDefs[i][0], err)
		}
		if err := acl.RestorePrivileges(dc, "public", functionDefs[i][0], acl.Function); err != nil {
			return err
		}
	}

	log.Trace("created database functions")
	return nil
}

func createFunction(dc *pgx.Conn, fname, fdef string) error {
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return fmt.Errorf("starting transaction for function: %w", err)
	}
	defer dbx.Rollback(tx)

	q := "DROP FUNCTION IF EXISTS public." + fname
	_, err = tx.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("dropping function: %w", err)
	}

	_, err = tx.Exec(context.TODO(), fdef)
	if err != nil {
		return fmt.Errorf("creating function: %w", err)
	}

	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}

	return nil
}
