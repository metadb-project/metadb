package server

import (
	"container/list"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/config"
	"github.com/metadb-project/metadb/cmd/metadb/jsonx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/types"
)

// rewriteCommandGraph transforms JSON objects contained in the root commands
// within a command graph.  The transformation is performed only for commands
// where there is exactly one JSON column in the command.
func rewriteCommandGraph(cat *catalog.Catalog, cmdgraph *command.CommandGraph) error {
	for e := cmdgraph.Commands.Front(); e != nil; e = e.Next() {
		// This is hardcoded to omit JSON transformation of certain FOLIO tables.  There
		// should be a general way for modules to control this, and then this could be
		// moved to the folio module.
		if (e.Value.(*command.Command)).SchemaName == "folio_source_record" &&
			((e.Value.(*command.Command)).TableName == "marc_records_lb" ||
				(e.Value.(*command.Command)).TableName == "edifact_records_lb") {
			continue
		}
		// Run the transform for a command.
		if err := rewriteCommand(cat, e); err != nil {
			log.Debug("%v", *(e.Value.(*command.Command)))
			return err
		}
	}
	return nil
}

// rewriteCommand transforms a JSON object located in a column within a command.
func rewriteCommand(cat *catalog.Catalog, cmde *list.Element) error {
	cmd := cmde.Value.(*command.Command)
	for i := range cmd.Column {
		// Check if this is a JSON column.
		if cmd.Column[i].DType == types.JSONType {
			// Check if this column is configured for transformation.
			path := config.NewJSONPath(cmd.SchemaName, cmd.TableName, cmd.Column[i].Name, "$")
			t := cat.JSONPathLookup(path)
			if t == "" {
				continue
			}
			// Begin transforming the record.
			if err := jsonx.RewriteJSON(cat, cmd, &(cmd.Column[i]), path, t); err != nil {
				return fmt.Errorf("rewriting json data: %s", err)
			}
		}
	}
	return nil
}
