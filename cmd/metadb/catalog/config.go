package catalog

import (
	"context"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func (c *Catalog) initConfig() error {
	rows, err := c.dp.Query(context.TODO(), "SELECT parameter, value FROM metadb.config")
	if err != nil {
		return fmt.Errorf("selecting config: %w", util.PGErr(err))
	}
	defer rows.Close()
	config := make(map[string]string)
	for rows.Next() {
		var p, v string
		if err := rows.Scan(&p, &v); err != nil {
			return fmt.Errorf("reading config: %w", util.PGErr(err))
		}
		config[p] = v
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading config: %w", util.PGErr(err))
	}
	c.config = config
	return nil
}

func (c *Catalog) SetConfig(parameter, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.config[parameter]
	if !ok {
		return fmt.Errorf("unknown parameter %q", parameter)
	}
	if _, err := c.dp.Exec(context.TODO(), "UPDATE metadb.config SET value=$1 WHERE parameter=$2", value, parameter); err != nil {
		return fmt.Errorf("writing config for parameter %q: %w", parameter, util.PGErr(err))
	}
	c.config[parameter] = value
	return nil
}

func (c *Catalog) GetConfig(parameter string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.config[parameter]
	if !ok {
		return "", fmt.Errorf("unknown parameter %q", parameter)
	}
	return value, nil
}

func (c *Catalog) IsConfigParameterValid(parameter string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.config[parameter]
	return ok
}
