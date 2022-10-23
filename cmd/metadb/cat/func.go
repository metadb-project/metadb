package cat

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

var functionDefs = [][]string{
	{"metadb_version()", `
CREATE FUNCTION public.metadb_version() RETURNS text
    AS $$ SELECT 'Metadb ` + util.MetadbVersion + `' $$
    LANGUAGE SQL`},
	{"list_authorizations()", `
CREATE FUNCTION public.list_authorizations()
    RETURNS TABLE (
        username text,
        note text
    )
    AS $$
    SELECT username,
           CASE WHEN (NOT dbupdated) THEN 'pending restart'
                WHEN (tables='.*' AND dbupdated) THEN 'authorized'
                ELSE 'not authorized'
           END note
    FROM metadb.auth
    $$ LANGUAGE SQL`},
	{"list_data_sources()", `
CREATE FUNCTION public.list_data_sources()
    RETURNS TABLE (
        name text,
        brokers text,
        security text,
        topics text,
        consumergroup text,
        schemapassfilter text,
        trimschemaprefix text,
        addschemaprefix text,
        module text
    )
    AS $$
    SELECT name,
           brokers,
           security,
           topics,
           consumergroup,
           schemapassfilter,
           trimschemaprefix,
           addschemaprefix,
           module
    FROM metadb.source
    $$ LANGUAGE SQL`},
	{"list_data_origins()", `
CREATE FUNCTION public.list_data_origins()
    RETURNS TABLE (
        name text
    )
    AS $$
    SELECT name
    FROM metadb.origin
    $$ LANGUAGE SQL`},
}

/*
  list_status_data_sources()
*/

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
			return err
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
		return fmt.Errorf("starting transaction for function: %s: %v", fname, err)
	}
	defer dbx.Rollback(tx)

	q := "DROP FUNCTION IF EXISTS public." + fname
	_, err = tx.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("dropping function: %s: %v", fname, err)
	}

	_, err = tx.Exec(context.TODO(), fdef)
	if err != nil {
		return fmt.Errorf("creating function: %s: %v", fname, err)
	}

	q = "REVOKE EXECUTE ON FUNCTION public." + fname + " FROM public;"
	_, err = tx.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("revoking public access to function: %s: %v", fname, err)
	}

	if fname == "metadb_version()" {
		for _, u := range users {
			q := "GRANT EXECUTE ON FUNCTION public." + fname + " TO " + u
			_, _ = tx.Exec(context.TODO(), q)
		}
	}

	err = tx.Commit(context.TODO())
	if err != nil {
		return err
	}
	return nil
}
