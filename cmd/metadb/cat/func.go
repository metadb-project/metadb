package cat

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

// TODO move to func and check existence and value of metadb_version() before creating;
// but note this requires user permissions to be fixed

var functionDefs = [][]string{
	{"metadb_version()",
		"CREATE OR REPLACE FUNCTION public.metadb_version() RETURNS text\n" +
			"    AS $$ SELECT 'Metadb " + util.MetadbVersion + "' $$\n" +
			"    LANGUAGE SQL;"},
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
		fname := f[0]
		fdef := f[1]
		_, err = dc.Exec(context.TODO(), fdef)
		if err != nil {
			return fmt.Errorf("creating function: %s: %v", fname, err)
		}
		for _, u := range users {
			q := "GRANT EXECUTE ON FUNCTION public." + fname + " TO " + u
			_, _ = dc.Exec(context.TODO(), q)
		}
	}

	for _, u := range users {
		q := "GRANT USAGE ON SCHEMA public TO " + u
		_, _ = dcsuper.Exec(context.TODO(), q)
	}

	return nil
}
