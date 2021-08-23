package server

import (
	"database/sql"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/cache"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/jsonx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
)

func rewriteCommandList(cl *command.CommandList, db *sql.DB, track *cache.Track, cschema *cache.Schema, database *sysdb.DatabaseConnector, rewriteJSON bool) error {
	for _, c := range cl.Cmd {
		// Rewrite command
		if err := rewriteCommand(cl, &c, db, rewriteJSON); err != nil {
			return fmt.Errorf("%s\n%v", err, c)
		}
	}
	return nil
}

func rewriteCommand(cl *command.CommandList, c *command.Command, db *sql.DB, rewriteJSON bool) error {
	for _, col := range c.Column {
		if rewriteJSON && col.DType == command.JSONType {
			if err := jsonx.RewriteJSON(cl, c, &col, db); err != nil {
				return err
			}
		}
	}
	return nil
}
