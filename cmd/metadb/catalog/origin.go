package catalog

import (
	"context"
	"fmt"
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func (c *Catalog) initOrigins() error {
	rows, err := c.dp.Query(context.TODO(), "SELECT name FROM metadb.origin")
	if err != nil {
		return fmt.Errorf("selecting origins: %w", util.PGErr(err))
	}
	defer rows.Close()
	origins := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("reading origin: %w", util.PGErr(err))
		}
		origins = append(origins, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("last error check: %w", util.PGErr(err))
	}
	c.origins = origins
	return nil
}

func (c *Catalog) DefineOrigin(origin string) error {
	if _, err := c.dp.Exec(context.TODO(), "INSERT INTO metadb.origin (name) VALUES ($1)", origin); err != nil {
		return fmt.Errorf("writing origin configuration: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.origins = append(c.origins, origin)
	return nil
}

func (c *Catalog) ExtractOrigin(schema string) (string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.origins {
		g := c.origins[i] + "_"
		if strings.HasPrefix(schema, g) {
			return c.origins[i], strings.TrimPrefix(schema, g)
		}
	}
	return "", schema
}
