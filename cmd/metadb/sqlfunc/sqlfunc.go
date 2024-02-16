package sqlfunc

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
	"github.com/nazgaret/metadb/cmd/metadb/catalog"
	"github.com/nazgaret/metadb/cmd/metadb/command"
	"github.com/nazgaret/metadb/cmd/metadb/dbx"
	"github.com/nazgaret/metadb/cmd/metadb/log"
	"github.com/nazgaret/metadb/cmd/metadb/util"
)

func SQLFunc(datadir string, cat *catalog.Catalog, db dbx.DB, url, ref, path, schema string, source string) error {
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
	rdir := filepath.Join(tmpdir, "sqlfunc")
	if err = os.RemoveAll(rdir); err != nil {
		return err
	}
	if _, err = git.PlainClone(rdir, false, &git.CloneOptions{
		URL:           url,
		ReferenceName: plumbing.ReferenceName(ref),
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
		if err = runFile(cat, url, ref, fullpath, dc, schema, file, source); err != nil {
			log.Warning("sqlfunc: %v: repository=%s ref=%s path=%s", err, url, ref, fullpath)
		}
		for _, u := range users {
			q = "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA " + schema + " TO " + u
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

func runFile(cat *catalog.Catalog, url, ref, fullpath string, dc *pgx.Conn, schema string, file string, source string) error {
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
	//start := time.Now()
	for _, l := range list {
		q := strings.TrimSpace(l)
		if q == "" {
			continue
		}
		if err = checkForDirectives(cat, url, ref, fullpath, q, &table, source); err != nil {
			return err
		}
		if _, err = tx.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("%s", strings.TrimPrefix(err.Error(), "ERROR: "))
		}
	}
	//elapsed := time.Since(start)
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	_ = schema
	//if table != "" {
	//	if err = cat.TableUpdatedNow(dbx.Table{Schema: schema, Table: table}, elapsed); err != nil {
	//		return fmt.Errorf("writing table updated time: %v", err)
	//	}
	//}
	return nil
}

var sqlSeparator = regexp.MustCompile("\\n\\s*\\n")

func checkForDirectives(cat *catalog.Catalog, url, ref, fullpath string, input string, table *string, source string) error {
	if !strings.HasPrefix(strings.TrimSpace(input), "--metadb:") {
		return nil
	}
	for _, l := range strings.Split(input, "\n") {
		line := strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(line, "--metadb:function "):
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
			requireTable := &dbx.Table{Schema: c[0], Table: c[1]}
			switch {
			case strings.HasSuffix(requireTable.Table, "__"):
				requireTable.Table = requireTable.Table[0 : len(requireTable.Table)-2]
			case strings.HasSuffix(requireTable.Table, "_"),
				strings.HasSuffix(requireTable.Table, "___"),
				strings.HasSuffix(requireTable.Table, "____"):
				log.Warning("sqlfunc: function name may be invalid in %q: repository=%s ref=%s path=%s",
					line, url, ref, fullpath)
				continue
			}
			requireColumn := c[2]
			requireColumnType := s[2]
			if requireTable.Schema == "" || requireTable.Table == "" || requireColumn == "" || requireColumnType == "" ||
				requireTable.Schema != strings.TrimSpace(requireTable.Schema) ||
				requireTable.Table != strings.TrimSpace(requireTable.Table) ||
				requireColumn != strings.TrimSpace(requireColumn) ||
				requireColumnType != strings.TrimSpace(requireColumnType) {
				log.Warning("sqlfunc: invalid identifier in %q: repository=%s ref=%s path=%s",
					line, url, ref, fullpath)
				continue
			}
			if cat.Column(&dbx.Column{Schema: requireTable.Schema, Table: requireTable.Table, Column: requireColumn}) != nil {
				continue
			}
			// Add table
			if !cat.TableExists(requireTable) {
				if err := cat.CreateNewTable(requireTable, false, &dbx.Table{}, source); err != nil {
					return fmt.Errorf("creating new table: %s: %v", requireTable, err)
				}
			}
			// Add column
			t := strings.ToLower(requireColumnType)
			if t == "text" || strings.HasPrefix(t, "varchar") {
				t = "text"
			}
			dtype, dtypesize := command.MakeDataType(t)
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
