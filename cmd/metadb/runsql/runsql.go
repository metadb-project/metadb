package runsql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

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
		fullpath := filepath.Join(path, f)
		if err = runFile(cat, url, tag, fullpath, dc, schema, file); err != nil {
			log.Warning("runsql: %v: repository=%s tag=%s path=%s", err, url, tag, fullpath)
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

func runFile(cat *catalog.Catalog, url, tag, fullpath string, dc *pgx.Conn, schema string, file string) error {
	var table string
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	list := sqlSeparator.Split(string(data), -1)
	tx, err := dc.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	start := time.Now()
	for _, l := range list {
		q := strings.TrimSpace(l)
		if q == "" {
			continue
		}
		if err = checkForDirectives(cat, url, tag, fullpath, q, &table); err != nil {
			return err
		}
		if _, err = tx.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("%s", strings.TrimPrefix(err.Error(), "ERROR: "))
		}
	}
	elapsed := time.Since(start)
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	if table != "" {
		if err = cat.TableUpdatedNow(dbx.Table{S: schema, T: table}, elapsed); err != nil {
			return fmt.Errorf("writing table updated time: %v", err)
		}
	}
	return nil
}

var sqlSeparator = regexp.MustCompile("\\n\\s*\\n")

func checkForDirectives(cat *catalog.Catalog, url, tag, fullpath string, input string, table *string) error {
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
			t := strings.TrimSpace(s[1])
			if !simpleTable.MatchString(t) || strings.HasPrefix(t, "__") {
				return fmt.Errorf("invalid table name in directive %q", line)
			}
			*table = t
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
				log.Warning("runsql: table name may be invalid in %q: repository=%s tag=%s path=%s",
					line, url, tag, fullpath)
				continue
			}
			requireColumn := c[2]
			requireColumnType := s[2]
			if requireTable.S == "" || requireTable.T == "" || requireColumn == "" || requireColumnType == "" ||
				requireTable.S != strings.TrimSpace(requireTable.S) ||
				requireTable.T != strings.TrimSpace(requireTable.T) ||
				requireColumn != strings.TrimSpace(requireColumn) ||
				requireColumnType != strings.TrimSpace(requireColumnType) {
				log.Warning("runsql: invalid identifier in %q: repository=%s tag=%s path=%s",
					line, url, tag, fullpath)
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
var simpleTable = regexp.MustCompile("^[A-Za-z_][0-9A-Za-z_]*$")
