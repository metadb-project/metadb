package catalog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
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

func SetSyncMode(dq dbx.Queryable, sync bool, source string) error {
	q := "UPDATE metadb.source SET sync=$1 WHERE name=$2"
	if _, err := dq.Exec(context.TODO(), q, sync, source); err != nil {
		return err
	}
	return nil
}

func IsSyncMode(dq dbx.Queryable, source string) (bool, error) {
	var sync bool
	q := "SELECT sync FROM metadb.source WHERE name=$1"
	err := dq.QueryRow(context.TODO(), q, source).Scan(&sync)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, fmt.Errorf("unable to query sync mode")
	case err != nil:
		return false, fmt.Errorf("querying sync mode: %s", err)
	default:
		return sync, nil
	}
}
