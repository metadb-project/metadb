package server

import (
	"container/list"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/jsonx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
)

func rewriteCommandList(cmdlist *list.List, rewriteJSON bool) error {
	for e := cmdlist.Front(); e != nil; e = e.Next() {
		// Rewrite command
		if err := rewriteCommand(cmdlist, e, rewriteJSON); err != nil {
			log.Debug("%v", *(e.Value.(*command.Command)))
			return fmt.Errorf("%v", err)
		}
	}
	return nil
}

func rewriteCommand(cmdlist *list.List, cmde *list.Element, rewriteJSON bool) error {
	// Rewrite JSON objects.
	columns := cmde.Value.(*command.Command).Column
	for i := range columns {
		col := columns[i]
		if rewriteJSON && col.DType == command.JSONType {
			if err := jsonx.RewriteJSON(cmdlist, cmde, &col); err != nil {
				return fmt.Errorf("rewriting json data: %s", err)
			}
		}
	}
	return nil
}
