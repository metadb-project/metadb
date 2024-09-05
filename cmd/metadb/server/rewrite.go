package server

import (
	"container/list"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/jsonx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
)

// rewriteCommandGraph transforms JSON objects contained in the root commands
// within a command graph.  The transformation is performed only for commands
// where there is exactly one JSON column in the command.
func rewriteCommandGraph(cmdgraph *command.CommandGraph) error {
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
		if err := rewriteCommand(e); err != nil {
			log.Debug("%v", *(e.Value.(*command.Command)))
			return err
		}
	}
	return nil
}

// rewriteCommand transforms a JSON object located in a column within a command.
// The transformation is performed only if there is exactly one JSON column in
// the command; otherwise no action is taken.
func rewriteCommand(cmde *list.Element) error {
	columns := cmde.Value.(*command.Command).Column
	var jsonColumn *command.CommandColumn
	for i := range columns {
		// Check if this is a JSON column.
		if columns[i].DType == command.JSONType {
			// Cancel transformation if more than one JSON column is present.
			if jsonColumn != nil {
				return nil
			}
			// Otherwise keep a pointer to this column.
			jsonColumn = &(columns[i])
		}
	}
	// If we have a JSON column, transform it.
	if jsonColumn != nil {
		if err := jsonx.RewriteJSON(cmde, jsonColumn); err != nil {
			return fmt.Errorf("rewriting json data: %s", err)
		}
	}
	return nil
}
