package server

import (
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/jsonx"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

func rewriteCommandList(cl *command.CommandList, rewriteJSON bool, db sqlx.DB) error {
	for _, c := range cl.Cmd {
		// Rewrite command
		if err := rewriteCommand(cl, &c, rewriteJSON, db); err != nil {
			return fmt.Errorf("%s\n%v", err, c)
		}
	}
	return nil
}

func rewriteCommand(cl *command.CommandList, c *command.Command, rewriteJSON bool, db sqlx.DB) error {
	for _, col := range c.Column {
		if rewriteJSON && col.DType == command.JSONType {
			if err := jsonx.RewriteJSON(cl, c, &col, db); err != nil {
				return fmt.Errorf("rewriting json data: %s", err)
			}
		}
	}
	return nil
}
