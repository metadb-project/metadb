package cache

import (
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/metadata"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

type Track struct {
	tables map[sqlx.Table]bool
	db     *sqlx.DB
}

func NewTrack(db *sqlx.DB) (*Track, error) {
	// read tracking tables
	tables, err := metadata.TrackRead(db)
	if err != nil {
		return nil, fmt.Errorf("reading track metadata: %s", err)
	}
	return &Track{tables: tables, db: db}, nil
}

func (t *Track) Contains(table *sqlx.Table) bool {
	return t.tables[*table]
}

func (t *Track) Add(table *sqlx.Table) error {
	t.tables[*table] = true
	if err := metadata.TrackWrite(t.db, table); err != nil {
		return fmt.Errorf("writing track metadata: %v: %s", table, err)
	}
	return nil
}
