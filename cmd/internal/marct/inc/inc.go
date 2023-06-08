package inc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/internal/marct/marc"
	"github.com/metadb-project/metadb/cmd/internal/marct/options"
	"github.com/metadb-project/metadb/cmd/internal/marct/util"
	"github.com/metadb-project/metadb/cmd/internal/uuid"
)

const schemaVersion int64 = 17
const cksumTable = "marctab.cksum"
const metadataTableS = "marctab"
const metadataTableT = "metadata"
const metadataTable = metadataTableS + "." + metadataTableT

func IncUpdateAvail(dc *pgx.Conn) (bool, error) {
	var err error
	// check if metadata table exists
	var q = "SELECT 1 FROM information_schema.tables WHERE table_schema = '" + metadataTableS + "' AND table_name = '" + metadataTableT + "';"
	var i int64
	err = dc.QueryRow(context.TODO(), q).Scan(&i)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// check if version matches
	q = "SELECT version FROM " + metadataTable + " ORDER BY version LIMIT 1;"
	var v int64
	err = dc.QueryRow(context.TODO(), q).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("version number not found")
	}
	if err != nil {
		return false, err
	}
	if v != schemaVersion {
		return false, nil
	}
	return true, nil
}

func CreateCksum(dbc *util.DBC, srsRecords, srsMarc, srsMarctab, srsMarcAttr string) error {
	var err error
	var tx pgx.Tx
	if tx, err = util.BeginTx(context.TODO(), dbc.Conn); err != nil {
		return err
	}
	defer tx.Rollback(context.TODO())
	// cksum
	var q = "DROP TABLE IF EXISTS " + cksumTable
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping checksum table: %s", err)
	}
	// Filter should match marc.getInstanceID()
	q = "CREATE TABLE " + cksumTable + " (id uuid NOT NULL,cksum text)"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating checksum table: %s", err)
	}
	q = "INSERT INTO " + cksumTable + " (id,cksum)" +
		" SELECT r.id::uuid, " + util.MD5(srsMarcAttr) + " cksum FROM " +
		srsRecords + " r JOIN " + srsMarc + " m ON r.id = m.id JOIN " +
		srsMarctab + " mt ON r.id::uuid = mt.srs_id WHERE r.state = 'ACTUAL' AND mt.field = '999' AND mt.sf = 'i' AND ind1 = 'f' AND ind2 = 'f' AND mt.content <> ''"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("writing data to checksum table: %s", err)
	}
	q = "ALTER TABLE " + cksumTable + " ADD CONSTRAINT cksum_pkey PRIMARY KEY (id)"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("indexing checksum table: %s", err)
	}
	// metadata
	q = "DROP TABLE IF EXISTS " + metadataTable
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping metadata table: %s", err)
	}
	q = "CREATE TABLE " + metadataTable + " AS SELECT " + strconv.FormatInt(schemaVersion, 10) + " AS version;"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating metadata table: %s", err)
	}
	// commit
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	return nil
}

func VacuumCksum(ctx context.Context, dbc *util.DBC) error {
	var err error
	if err = util.Vacuum(ctx, dbc, cksumTable); err != nil {
		return err
	}
	return nil
}

func IncrementalUpdate(opts *options.Options, connString string, srsRecords, srsMarc, srsMarcAttr, tablefinal string,
	printerr func(string, ...any), verbose int) error {

	var err error
	startUpdate := time.Now()
	conn, err := util.ConnectDB(context.TODO(), connString)
	if err != nil {
		return err
	}
	defer conn.Close(context.TODO())
	dbc := &util.DBC{
		Conn:       conn,
		ConnString: connString,
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Hour))
	defer cancel()
	// Vacuum in case previous run was not completed.
	// _ = util.Vacuum(ctx, dbc, tablefinal)
	// _ = VacuumCksum(ctx, dbc)
	// add new data
	if err = updateNew(ctx, opts, dbc, srsRecords, srsMarc, srsMarcAttr, tablefinal, printerr,
		verbose); err != nil {
		return fmt.Errorf("new: %s", err)
	}
	// remove deleted data
	if err = updateDelete(ctx, dbc, srsRecords, tablefinal, printerr, verbose); err != nil {
		return fmt.Errorf("delete: %s", err)
	}
	// replace modified data
	if err = updateChange(ctx, opts, dbc, srsRecords, srsMarc, srsMarcAttr, tablefinal, printerr, verbose); err != nil {
		return fmt.Errorf("change: %s", err)
	}
	// vacuum
	/*
		startVacuum := time.Now()
		if err = util.Vacuum(ctx, dbc, tablefinal); err != nil {
			return fmt.Errorf("vacuum: %s", err)
		}
		if err = VacuumCksum(ctx, dbc); err != nil {
			return fmt.Errorf("vacuum cksum: %s", err)
		}
		if verbose >= 1 {
			printerr(" %s vacuum", util.ElapsedTime(startVacuum))
		}
	*/
	if verbose >= 1 {
		printerr("%s incremental update", util.ElapsedTime(startUpdate))
	}
	return nil
}

func updateNew(ctx context.Context, opts *options.Options, dbc *util.DBC, srsRecords, srsMarc, srsMarcAttr,
	tablefinal string, printerr func(string, ...any), verbose int) error {
	startNew := time.Now()
	var err error
	// find new data
	_, _ = dbc.Conn.Exec(ctx, "DROP TABLE IF EXISTS marctab.inc_add")
	var q = "CREATE UNLOGGED TABLE marctab.inc_add AS SELECT r.id::uuid FROM " + srsRecords + " r LEFT JOIN " +
		cksumTable + " c ON r.id::uuid = c.id WHERE c.id IS NULL;"
	if _, err = dbc.Conn.Exec(ctx, q); err != nil {
		return fmt.Errorf("creating addition table: %s", err)
	}
	//if err = util.VacuumAnalyze(ctx, dbc, "marctab.inc_add"); err != nil {
	//	return fmt.Errorf("vacuum analyze: %s", err)
	//}
	q = "ALTER TABLE marctab.inc_add ADD CONSTRAINT marctab_add_pkey PRIMARY KEY (id);"
	if _, err = dbc.Conn.Exec(ctx, q); err != nil {
		return fmt.Errorf("creating primary key on addition table: %s", err)
	}
	var connw *pgx.Conn
	if connw, err = util.ConnectDB(ctx, dbc.ConnString); err != nil {
		return err
	}
	defer connw.Close(ctx)
	var tx pgx.Tx
	if tx, err = util.BeginTx(ctx, connw); err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// transform
	sfmap := make(map[util.FieldSF]struct{})
	q = filterQuery(srsRecords, srsMarc, srsMarcAttr, "marctab.inc_add")
	var rows pgx.Rows
	if rows, err = dbc.Conn.Query(ctx, q); err != nil {
		return fmt.Errorf("selecting records to add: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, matchedID, instanceHRID, state, data *string
		var cksum string
		if err = rows.Scan(&id, &matchedID, &instanceHRID, &state, &data, &cksum); err != nil {
			return err
		}
		var instanceID string
		var mrecs []marc.Marc
		var skip bool
		id, matchedID, instanceHRID, instanceID, mrecs, skip = util.Transform(id, matchedID, instanceHRID,
			state, data, printerr, verbose)
		if skip {
			continue
		}
		if _, err = uuid.EncodeUUID(instanceID); err != nil {
			printerr("id=%s: encoding instance_id %q: %v", *id, instanceID, err)
			instanceID = uuid.NilUUID
		}
		var m marc.Marc
		for _, m = range mrecs {
			// Create partition.
			fieldSF := util.FieldSF{Field: m.Field, SF: m.SF}
			_, ok := sfmap[fieldSF]
			if !ok {
				t := opts.SFPartitionTable(fieldSF.Field, fieldSF.SF)
				q = "CREATE TABLE IF NOT EXISTS " + opts.FinalPartitionSchema + "." + t +
					" PARTITION OF " + opts.FinalPartitionSchema + "." +
					opts.PartitionTableBase + fieldSF.Field +
					" FOR VALUES IN ('" + fieldSF.SF + "')"
				if _, err = tx.Exec(ctx, q); err != nil {
					return fmt.Errorf("adding partition: %v", err)
				}
				sfmap[fieldSF] = struct{}{}
			}
			// Insert row.
			q = "INSERT INTO " + tablefinal + " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)"
			if _, err = tx.Exec(ctx, q, id, m.Line, matchedID, instanceHRID, instanceID, m.Field, m.Ind1,
				m.Ind2, m.Ord, m.SF, m.Content); err != nil {
				return fmt.Errorf("adding record: %v", err)
			}
		}
		// cksum
		if len(mrecs) != 0 {
			q = "INSERT INTO " + cksumTable + " VALUES($1,$2)"
			if _, err = tx.Exec(ctx, q, id, cksum); err != nil {
				return fmt.Errorf("adding checksum: %v", err)
			}
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS marctab.inc_add"); err != nil {
		return fmt.Errorf("dropping addition table: %s", err)
	}
	if verbose >= 1 {
		printerr(" %s new", util.ElapsedTime(startNew))
	}
	return nil
}

func updateDelete(ctx context.Context, dbc *util.DBC, srsRecords, tablefinal string, printerr func(string, ...any), verbose int) error {
	startDelete := time.Now()
	var err error
	// find deleted data
	_, _ = dbc.Conn.Exec(ctx, "DROP TABLE IF EXISTS marctab.inc_delete")
	q := "CREATE UNLOGGED TABLE marctab.inc_delete AS SELECT c.id FROM " + srsRecords + " r RIGHT JOIN " +
		cksumTable + " c ON r.id::uuid = c.id WHERE r.id IS NULL;"
	if _, err = dbc.Conn.Exec(ctx, q); err != nil {
		return fmt.Errorf("creating deletion table: %s", err)
	}
	//if err = util.VacuumAnalyze(ctx, dbc, "marctab.inc_delete"); err != nil {
	//	return fmt.Errorf("vacuum analyze: %s", err)
	//}
	q = "ALTER TABLE marctab.inc_delete ADD CONSTRAINT marctab_delete_pkey PRIMARY KEY (id);"
	if _, err = dbc.Conn.Exec(ctx, q); err != nil {
		return fmt.Errorf("creating primary key on deletion table: %s", err)
	}
	if verbose >= 2 {
		// show changes
		q = "SELECT id FROM marctab.inc_delete;"
		var rows pgx.Rows
		if rows, err = dbc.Conn.Query(ctx, q); err != nil {
			return fmt.Errorf("reading deletion list: %s", err)
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				return fmt.Errorf("reading deletion ID: %s", err)
			}
			if verbose >= 1 {
				printerr("removing: id=%s", id)
			}
		}
		if err = rows.Err(); err != nil {
			return fmt.Errorf("reading deletion rows: %s", err)
		}
		rows.Close()
	}
	var connw *pgx.Conn
	if connw, err = util.ConnectDB(ctx, dbc.ConnString); err != nil {
		return fmt.Errorf("opening connection for writing: %v", err)
	}
	defer connw.Close(ctx)
	var tx pgx.Tx
	if tx, err = util.BeginTx(ctx, connw); err != nil {
		return fmt.Errorf("opening transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	// delete in finaltable
	q = "DELETE FROM " + tablefinal + " WHERE srs_id IN (SELECT id FROM marctab.inc_delete);"
	if _, err = tx.Exec(ctx, q); err != nil {
		return fmt.Errorf("deleting records: %s", err)
	}
	// delete in cksum table
	q = "DELETE FROM " + cksumTable + " WHERE id IN (SELECT id FROM marctab.inc_delete);"
	if _, err = tx.Exec(ctx, q); err != nil {
		return fmt.Errorf("deleting cksum: %s", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing updates: %v", err)
	}
	if _, err = dbc.Conn.Exec(ctx, "DROP TABLE IF EXISTS marctab.inc_delete"); err != nil {
		return fmt.Errorf("dropping deletion table: %s", err)
	}
	if verbose >= 1 {
		printerr(" %s delete", util.ElapsedTime(startDelete))
	}
	return nil
}

func updateChange(ctx context.Context, opts *options.Options, dbc *util.DBC, srsRecords, srsMarc, srsMarcAttr, tablefinal string, printerr func(string, ...any), verbose int) error {
	startChange := time.Now()
	var err error
	// find changed data
	_, _ = dbc.Conn.Exec(ctx, "DROP TABLE IF EXISTS marctab.inc_change")
	var q = "CREATE UNLOGGED TABLE marctab.inc_change AS SELECT r.id::uuid FROM " + srsRecords + " r JOIN " + cksumTable + " c ON r.id::uuid = c.id JOIN " + srsMarc + " m ON r.id = m.id WHERE " + util.MD5(srsMarcAttr) + " <> c.cksum;"
	if _, err = dbc.Conn.Exec(ctx, q); err != nil {
		return fmt.Errorf("creating change table: %s", err)
	}
	//if err = util.VacuumAnalyze(ctx, dbc, "marctab.inc_change"); err != nil {
	//	return fmt.Errorf("vacuum analyze: %s", err)
	//}
	q = "ALTER TABLE marctab.inc_change ADD CONSTRAINT marctab_change_pkey PRIMARY KEY (id);"
	if _, err = dbc.Conn.Exec(ctx, q); err != nil {
		return fmt.Errorf("creating primary key on change table: %s", err)
	}
	// connR is used for queries concurrent with reading rows.
	var connR *pgx.Conn
	if connR, err = util.ConnectDB(ctx, dbc.ConnString); err != nil {
		return fmt.Errorf("opening connection for reading: %s", err)
	}
	defer connR.Close(ctx)
	// connW is used to write the changes.
	var connW *pgx.Conn
	if connW, err = util.ConnectDB(ctx, dbc.ConnString); err != nil {
		return fmt.Errorf("opening connection for writing: %s", err)
	}
	defer connW.Close(ctx)
	var tx pgx.Tx
	if tx, err = util.BeginTx(ctx, connW); err != nil {
		return fmt.Errorf("opening transaction: %s", err)
	}
	defer tx.Rollback(ctx)
	// transform
	sfmap := make(map[util.FieldSF]struct{})
	q = filterQuery(srsRecords, srsMarc, srsMarcAttr, "marctab.inc_change")
	var rows pgx.Rows
	if rows, err = dbc.Conn.Query(ctx, q); err != nil {
		return fmt.Errorf("selecting records to change: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, matchedID, instanceHRID, state, data *string
		var cksum string
		if err = rows.Scan(&id, &matchedID, &instanceHRID, &state, &data, &cksum); err != nil {
			return fmt.Errorf("reading changes: %s", err)
		}
		var instanceID string
		var mrecs []marc.Marc
		var skip bool
		id, matchedID, instanceHRID, instanceID, mrecs, skip = util.Transform(id, matchedID, instanceHRID, state, data, printerr, verbose)
		if skip {
			continue
		}
		if _, err = uuid.EncodeUUID(instanceID); err != nil {
			printerr("id=%s: encoding instance_id %q: %v", *id, instanceID, err)
			instanceID = uuid.NilUUID
		}
		// check if there are existing rows in tablefinal
		var exist bool
		var i int64
		q = "SELECT 1 FROM " + tablefinal + " WHERE srs_id=$1 LIMIT 1"
		err = connR.QueryRow(ctx, q, *id).Scan(&i)
		switch {
		case err == pgx.ErrNoRows:
		case err != nil:
			return fmt.Errorf("checking for existing rows: %s", err)
		default:
			exist = true
		}
		// delete in tablefinal
		q = "DELETE FROM " + tablefinal + " WHERE srs_id=$1"
		if _, err = tx.Exec(ctx, q, *id); err != nil {
			return fmt.Errorf("deleting record (change): %s", err)
		}
		// delete in cksum table
		q = "DELETE FROM " + cksumTable + " WHERE id=$1"
		if _, err = tx.Exec(ctx, q, *id); err != nil {
			return fmt.Errorf("deleting checksum (change): %s", err)
		}
		var m marc.Marc
		for _, m = range mrecs {
			// Create partition.
			fieldSF := util.FieldSF{Field: m.Field, SF: m.SF}
			_, ok := sfmap[fieldSF]
			if !ok {
				t := opts.SFPartitionTable(fieldSF.Field, fieldSF.SF)
				q = "CREATE TABLE IF NOT EXISTS " + opts.FinalPartitionSchema + "." + t +
					" PARTITION OF " + opts.FinalPartitionSchema + "." +
					opts.PartitionTableBase + fieldSF.Field +
					" FOR VALUES IN ('" + fieldSF.SF + "')"
				if _, err = tx.Exec(ctx, q); err != nil {
					return fmt.Errorf("adding partition: %v", err)
				}
				sfmap[fieldSF] = struct{}{}
			}
			// Insert row.
			q = "INSERT INTO " + tablefinal + " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)"
			_, err = tx.Exec(ctx, q, id, m.Line, matchedID, instanceHRID, instanceID, m.Field, m.Ind1,
				m.Ind2, m.Ord, m.SF, m.Content)
			if err != nil {
				return fmt.Errorf("rewriting record: %s", err)
			}
		}
		if verbose >= 2 && exist && len(mrecs) == 0 {
			printerr("removing: id=%s", *id)
		}
		// cksum
		if len(mrecs) != 0 {
			q = "INSERT INTO " + cksumTable + " VALUES($1,$2)"
			if _, err = tx.Exec(ctx, q, id, cksum); err != nil {
				return fmt.Errorf("rewriting checksum: %s", err)
			}
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if _, err = dbc.Conn.Exec(ctx, "DROP TABLE IF EXISTS marctab.inc_change"); err != nil {
		return fmt.Errorf("dropping change table: %s", err)
	}
	if verbose >= 1 {
		printerr(" %s modify", util.ElapsedTime(startChange))
	}
	return nil
}

func filterQuery(srsRecords, srsMarc, srsMarcAttr, filter string) string {
	return "" +
		"SELECT r.id::uuid, r.matched_id::uuid, r.external_hrid instance_hrid, r.state, m." + srsMarcAttr + "::text, " + util.MD5(srsMarcAttr) + " cksum " +
		"    FROM " + srsRecords + " r " +
		"        JOIN " + filter + " f ON r.id::uuid = f.id " +
		"        JOIN " + srsMarc + " m ON r.id = m.id;"
}
