package catalog

import (
	"context"
	"fmt"
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/types"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func (c *Catalog) initJSON() error {
	q := "SELECT schema_name, table_name, column_name, path, map FROM metadb.transform_json"
	rows, err := c.dp.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting json configuration: %w", err)
	}
	defer rows.Close()
	t := make(map[types.JSONPath]string)
	for rows.Next() {
		var schema, table, column, path, tmap string
		err := rows.Scan(&schema, &table, &column, &path, &tmap)
		if err != nil {
			return fmt.Errorf("reading json configuration: %w", err)
		}
		t[types.NewJSONPath(schema, table, column, path)] = tmap
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading json configuration: %w", err)
	}
	c.jsonTransform = t
	return nil
}

func (c *Catalog) JSONPathLookup(path types.JSONPath) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.jsonTransform[path]
}

func (c *Catalog) DefineJSONMapping(schema, table, column, path, mapping string) error {
	if err := writeJSONMapping(c, schema, table, column, path, mapping); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.jsonTransform[types.NewJSONPath(schema, table, column, path)] = mapping
	return nil
}

func writeJSONMapping(c *Catalog, schema, table, column, path, mapping string) error {
	if _, err := c.dp.Exec(context.TODO(),
		"INSERT INTO metadb.transform_json (schema_name, table_name, column_name, path, map) VALUES ($1, $2, $3, $4, $5)",
		schema, table, column, path, mapping); err != nil {
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
			return fmt.Errorf("JSON mapping from (table \"%s.%s\", column %q, path %q) to (%q) conflicts with an existing mapping",
				schema, table, column, path, mapping)
		}
		return util.PGErr(err)
	}
	return nil
}
