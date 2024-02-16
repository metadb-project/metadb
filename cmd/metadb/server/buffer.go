package server

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nazgaret/metadb/cmd/metadb/catalog"
	"github.com/nazgaret/metadb/cmd/metadb/dbx"
	"github.com/nazgaret/metadb/cmd/metadb/dsync"
	"github.com/nazgaret/metadb/cmd/metadb/log"
)

type execbuffer struct {
	ctx context.Context
	dp  *pgxpool.Pool
	// syncIDs is a map of buffered IDs ready for COPY to sync tables.
	syncIDs map[dbx.Table][][]any
	// mergeData is a slice of buffered update-insert SQL statement pairs.
	mergeData map[dbx.Table][][]string
	syncMode  dsync.Mode
}

func (e *execbuffer) queueSyncID(table *dbx.Table, id int64) {
	e.syncIDs[*table] = append(e.syncIDs[*table], []any{id})
}

func (e *execbuffer) queueMergeData(table *dbx.Table, update, insert *string) {
	e.mergeData[*table] = append(e.mergeData[*table], []string{*update, *insert})
}

func (e *execbuffer) flush() error {
	// Flush merge data.
	log.Trace("FLUSH merge data")
	if err := e.flushMergeData(); err != nil {
		return fmt.Errorf("flushing exec buffer: writing merge data: %v", err)
	}
	// Flush sync IDs.
	log.Trace("FLUSH sync IDs")
	if err := e.flushSyncIDs(); err != nil {
		return fmt.Errorf("flushing exec buffer: writing to sync tables: %v", err)
	}
	return nil
}

func (e *execbuffer) flushSyncIDs() error {
	for t, a := range e.syncIDs {
		synct := catalog.SyncTable(&t)
		copyCount, err := e.dp.CopyFrom(
			e.ctx,
			pgx.Identifier{synct.Schema, synct.Table},
			[]string{"__id"},
			pgx.CopyFromRows(a),
		)
		if err != nil {
			return fmt.Errorf("copy to sync table: %v", err)
		}
		log.Trace("copy %d rows to table %q", copyCount, synct)
	}
	e.syncIDs = make(map[dbx.Table][][]any) // Clear buffers.
	return nil
}

func (e *execbuffer) flushMergeData() error {
	batchSize := 100
	for t, a := range e.mergeData {
		lena := len(a)
		for i := 0; i < lena; i += batchSize {
			batchEndIndex := min(i+batchSize, lena)
			actualBatchSize := batchEndIndex - i
			batch := pgx.Batch{}
			ids := make([][]any, actualBatchSize)
			for j := range ids {
				ids[j] = []any{0}
			}
			for k := i; k < batchEndIndex; k++ {
				// Queue UPDATE.
				batch.Queue(a[k][0])
				// Queue INSERT.
				p := &(ids[k-i][0])
				batch.Queue(a[k][1]).QueryRow(func(row pgx.Row) error {
					return row.Scan(p)
				})
			}
			if err := e.dp.SendBatch(e.ctx, &batch).Close(); err != nil {
				return fmt.Errorf("update and insert: %v", err)
			}
			// If resync mode, flush IDs to sync table.
			if e.syncMode == dsync.Resync {
				//e.queueSyncID(&t, id)
				synct := catalog.SyncTable(&t)
				copyCount, err := e.dp.CopyFrom(
					e.ctx,
					pgx.Identifier{synct.Schema, synct.Table},
					[]string{"__id"},
					pgx.CopyFromRows(ids),
				)
				if err != nil {
					return fmt.Errorf("copy to sync table: %v", err)
				}
				log.Trace("copy %d rows to table %q", copyCount, synct)
			}
		}
	}
	e.mergeData = make(map[dbx.Table][][]string) // Clear buffers.
	return nil
}
