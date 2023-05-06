package catalog

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"golang.org/x/net/context"
)

func (c *Catalog) initSnapshot() {
	c.reset()
}

func (c *Catalog) ResetLastSnapshotRecord() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reset()
}

func (c *Catalog) reset() {
	c.lastSnapshotRecord = time.Now()
}

func (c *Catalog) HoursSinceLastSnapshotRecord() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Since(c.lastSnapshotRecord).Hours()
}

func SetResyncMode(dq dbx.Queryable, resync bool) error {
	q := "UPDATE metadb.init SET resync=$1"
	if _, err := dq.Exec(context.TODO(), q, resync); err != nil {
		return err
	}
	return nil
}

func IsResyncMode(dq dbx.Queryable) (bool, error) {
	var resync bool
	err := dq.QueryRow(context.TODO(), "SELECT resync FROM metadb.init").Scan(&resync)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, fmt.Errorf("unable to query resync mode")
	case err != nil:
		return false, fmt.Errorf("querying resync mode: %s", err)
	default:
		return resync, nil
	}
}
