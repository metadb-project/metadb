package runsql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func RunSQL(datadir string, cat *catalog.Catalog, db dbx.DB, url, tag, path, schema string) error {
	dc, err := db.Connect()
	if err != nil {
		return err
	}
	defer dbx.Close(dc)
	users, err := catalog.AllUsers(dc)
	if err != nil {
		return err
	}
	q := "CREATE SCHEMA IF NOT EXISTS " + schema
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	for _, u := range users {
		q = "GRANT USAGE ON SCHEMA " + schema + " TO " + u
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			return err
		}
	}
	q = "SET search_path = " + schema
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return err
	}

	tmpdir := filepath.Join(datadir, "tmp")
	if err = os.MkdirAll(tmpdir, util.ModePermRWX); err != nil {
		return err
	}
	rdir := filepath.Join(tmpdir, "runsql")
	if err = os.RemoveAll(rdir); err != nil {
		return err
	}
	if _, err = git.PlainClone(rdir, false, &git.CloneOptions{
		URL:           url,
		ReferenceName: plumbing.ReferenceName("refs/tags/" + tag),
		SingleBranch:  true,
		Depth:         1,
		Progress:      nil,
		Tags:          git.NoTags,
	}); err != nil {
		return err
	}
	workdir := filepath.Join(rdir, path)
	var data []byte
	data, err = os.ReadFile(filepath.Join(workdir, "runlist.txt"))
	if err != nil {
		return err
	}
	list := strings.Split(string(data), "\n")
	for i, l := range list {
		f := strings.TrimSpace(l)
		if f == "" {
			continue
		}
		log.Trace("running file: %d %s", i, f)
		file := filepath.Join(workdir, f)
		if err = runFile(cat, dc, schema, file); err != nil {
			log.Error("%v: repository=%s tag=%s path=%s", err, url, tag, filepath.Join(path, f))
		}
		for _, u := range users {
			q = "GRANT SELECT ON ALL TABLES IN SCHEMA " + schema + " TO " + u
			if _, err = dc.Exec(context.TODO(), q); err != nil {
				return err
			}
		}
	}
	if err := os.RemoveAll(rdir); err != nil {
		return err
	}
	return nil
}

func runFile(cat *catalog.Catalog, dc *pgx.Conn, schema string, file string) error {
	var table string
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	list := sqlSeparator.Split(string(data), -1)
	for _, l := range list {
		q := strings.TrimSpace(l)
		if q == "" {
			continue
		}
		if err = checkForDirectives(cat, q, &table); err != nil {
			return err
		}
		if _, err = dc.Exec(context.TODO(), q); err != nil {
			if table != "" {
				_ = cat.TableUpdatedNow(dbx.Table{S: schema, T: table})
			}
			return fmt.Errorf("%s", strings.TrimPrefix(err.Error(), "ERROR: "))
		}
	}
	if table != "" {
		if err = cat.TableUpdatedNow(dbx.Table{S: schema, T: table}); err != nil {
			return fmt.Errorf("writing table updated time: %v", err)
		}
	}
	return nil
}

var sqlSeparator = regexp.MustCompile("\\n\\s*\\n")

func checkForDirectives(cat *catalog.Catalog, input string, table *string) error {
	if !strings.HasPrefix(strings.TrimSpace(input), "--metadb:") {
		return nil
	}
	for _, l := range strings.Split(input, "\n") {
		line := strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(line, "--metadb:table "):
			s := spaceSeparator.Split(line, -1)
			if len(s) < 2 {
				return fmt.Errorf("syntax error in directive %q", line)
			}
			*table = strings.TrimSpace(s[1])
		case strings.HasPrefix(line, "--metadb:require "):
			s := spaceSeparator.Split(line, -1)
			if len(s) < 3 {
				return fmt.Errorf("syntax error in directive %q", line)
			}
			c := strings.Split(s[1], ".")
			if len(c) < 3 {
				return fmt.Errorf("syntax error in directive %q", line)
			}
			requireTable := dbx.Table{S: c[0], T: c[1]}
			switch {
			case strings.HasSuffix(requireTable.T, "__"):
				requireTable.T = requireTable.T[0 : len(requireTable.T)-2]
			case strings.HasSuffix(requireTable.T, "_"),
				strings.HasSuffix(requireTable.T, "___"),
				strings.HasSuffix(requireTable.T, "____"):
				log.Warning("table name may be invalid in %q", line)
				continue
			}
			requireColumn := c[2]
			requireColumnType := s[2]
			if requireTable.S == "" || requireTable.T == "" || requireColumn == "" || requireColumnType == "" ||
				requireTable.S != strings.TrimSpace(requireTable.S) ||
				requireTable.T != strings.TrimSpace(requireTable.T) ||
				requireColumn != strings.TrimSpace(requireColumn) ||
				requireColumnType != strings.TrimSpace(requireColumnType) {
				log.Error("invalid identifier in %q", line)
				continue
			}
			if cat.Column(&sqlx.Column{
				Schema: requireTable.S,
				Table:  requireTable.T,
				Column: requireColumn,
			}) != nil {
				continue
			}
			// Add table
			if err := cat.CreateNewTable(requireTable, false, dbx.Table{}); err != nil {
				return fmt.Errorf("creating new table: %s: %v", requireTable, err)
			}
			// Add column
			t := strings.ToLower(requireColumnType)
			if t == "text" || strings.HasPrefix(t, "varchar(") {
				t = "varchar"
			}
			dtype, dtypesize := command.MakeDataType(t, 1)
			if err := cat.AddColumn(requireTable, requireColumn, dtype, dtypesize); err != nil {
				return fmt.Errorf("creating new column: %s.%s: %v", requireTable, requireColumn, err)
			}
			log.Debug("created column %s.%s", requireTable, requireColumn)
		}

	}
	return nil
}

var spaceSeparator = regexp.MustCompile("\\s+")
