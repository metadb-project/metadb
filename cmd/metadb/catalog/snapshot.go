package catalog

import (
	"time"
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
