package catalog

import (
	"context"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/config"
)

func (c *Catalog) initJSON() error {
	q := "SELECT schema_name, table_name, column_name, path, map FROM metadb.transform_json"
	rows, err := c.dp.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting json configuration: %w", err)
	}
	defer rows.Close()
	t := make(map[config.JSONPath]string)
	for rows.Next() {
		var schema, table, column, path, tmap string
		err := rows.Scan(&schema, &table, &column, &path, &tmap)
		if err != nil {
			return fmt.Errorf("reading json configuration: %w", err)
		}
		t[config.NewJSONPath(schema, table, column, path)] = tmap
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading json configuration: %w", err)
	}
	c.jsonTransform = t
	return nil
}

func (c *Catalog) JSONPathLookup(path config.JSONPath) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.jsonTransform[path]
}
