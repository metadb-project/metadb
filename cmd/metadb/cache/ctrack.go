package cache

/*
import (
	"fmt"

	"github.com/nazgaret/metadb/cmd/metadb/metadata"

	"github.com/nazgaret/metadb/cmd/metadb/sqlx"
)

type Track struct {
	tables map[sqlx.Table]bool
	db     sqlx.DB
}

func NewTrack(db sqlx.DB) (*Track, error) {
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

func (t *Track) All() []sqlx.Table {
	var a []sqlx.Table
	for table, b := range t.tables {
		if b {
			a = append(a, table)
		}
	}
	return a
}

func (t *Track) Add(table *sqlx.Table, transformed bool, parentTable *sqlx.Table) error {
	t.tables[*table] = true
	if err := metadata.TrackWrite(t.db, table, transformed, parentTable); err != nil {
		return fmt.Errorf("writing track metadata: %v: %s", table, err)
	}
	return nil
}
*/
