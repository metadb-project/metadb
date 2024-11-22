package libmarct

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/internal/libmarct/inc"
	"github.com/metadb-project/metadb/cmd/internal/libmarct/local"
	"github.com/metadb-project/metadb/cmd/internal/libmarct/marc"
	"github.com/metadb-project/metadb/cmd/internal/libmarct/options"
	"github.com/metadb-project/metadb/cmd/internal/libmarct/util"
	"github.com/spf13/viper"
	"gopkg.in/ini.v1"
)

type MARCTransform struct {
	FullUpdate bool
	Datadir    string
	Users      []string
	//TrigramIndex bool
	//NoIndexes    bool
	Verbose int // 0=quiet, 1=summary, 2=detail
	//CSVFileName  string
	SRSRecords  string
	SRSMarc     string
	SRSMarcAttr string
	Metadb      bool
	// Move into options package:
	PrintErr PrintErr
	Loc      Locations
}

type PrintErr func(string, ...interface{})

type Locations struct {
	SrsRecords       string
	SrsMarc          string
	SrsMarcAttr      string
	TablefinalSchema string
	TablefinalTable  string
}

var tableoutSchema = "marctab"
var tableoutTable = "_mt"
var tableout = tableoutSchema + "." + tableoutTable

var allFields = util.GetAllFieldNames()

//var csvFile *os.File

func (t *MARCTransform) Transform() error {
	t.Loc = setupLocations(t)
	opts := &options.Options{
		TempPartitionSchema:  tableoutSchema,
		TempTablePrefix:      "_",
		PartitionTableBase:   "mt",
		FinalPartitionSchema: tableoutSchema,
	}
	return marcTransform(opts, t)
}

func marcTransform(opts *options.Options, marct *MARCTransform) error {
	// Read database configuration
	var host, port, user, password, dbname, sslmode string
	var err error
	if marct.Metadb {
		host, port, user, password, dbname, sslmode, err = readConfigMetadb(marct)
		if err != nil {
			return err
		}
	} else {
		host, port, user, password, dbname, sslmode, err = readConfigLDP1(marct)
		if err != nil {
			return err
		}
	}
	connString := "host=" + host + " port=" + port + " user=" + user + " password=" + password + " dbname=" +
		dbname + " sslmode=" + sslmode
	conn, err := util.ConnectDB(context.TODO(), connString)
	if err != nil {
		return err
	}
	defer conn.Close(context.TODO())
	if err = setupSchema(conn); err != nil {
		return fmt.Errorf("setting up schema: %v", err)
	}
	var incUpdateAvail bool
	if incUpdateAvail, err = inc.IncUpdateAvail(conn); err != nil {
		return err
	}
	var retry bool
	for {
		if !retry && incUpdateAvail && !marct.FullUpdate {
			if marct.Verbose >= 1 {
				marct.PrintErr("starting incremental update")
			}
			err = inc.IncrementalUpdate(opts, connString, marct.Loc.SrsRecords, marct.Loc.SrsMarc, marct.Loc.SrsMarcAttr,
				marct.Loc.tablefinal(), marct.PrintErr, marct.Verbose)
			if err != nil {
				marct.PrintErr("restarting with full update due to early termination: %v", err)
				retry = true
			}
		} else {
			retry = false
			if marct.Verbose >= 1 {
				marct.PrintErr("starting full update")
			}
			if err = fullUpdate(opts, marct, connString, marct.PrintErr); err != nil {
				if _, errd := conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout); errd != nil {
					return fmt.Errorf("dropping table %q: %v", tableout, errd)
				}
				return err
			}
		}
		if !retry {
			break
		}
	}
	return nil
}

func setupLocations(opts *MARCTransform) Locations {
	loc := Locations{
		SrsRecords:       "folio_source_record.records_lb",
		SrsMarc:          "folio_source_record.marc_records_lb",
		SrsMarcAttr:      "content",
		TablefinalSchema: "folio_source_record",
		TablefinalTable:  "marc__t",
	}
	if !opts.Metadb { // LDP1
		loc.SrsRecords = "public.srs_records"
		loc.SrsMarc = "public.srs_marc"
		loc.SrsMarcAttr = "data"
		loc.TablefinalSchema = "public"
		loc.TablefinalTable = "srs_marctab"
	}
	if opts.SRSRecords != "" {
		loc.SrsRecords = opts.SRSRecords
	}
	if opts.SRSMarc != "" {
		loc.SrsMarc = opts.SRSMarc
	}
	if opts.SRSMarcAttr != "" {
		loc.SrsMarcAttr = opts.SRSMarcAttr
	}
	return loc
}

func setupSchema(dc *pgx.Conn) error {
	var err error
	if _, err = dc.Exec(context.TODO(), "CREATE SCHEMA IF NOT EXISTS "+tableoutSchema); err != nil {
		return fmt.Errorf("creating schema: %s", err)
	}
	var q = "COMMENT ON SCHEMA " + tableoutSchema + " IS 'system tables for MARC transform'"
	if _, err = dc.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("adding comment on schema: %s", err)
	}
	return nil
}

func fullUpdate(opts *options.Options, marct *MARCTransform, connString string, printerr PrintErr) error {
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
	// tableTemps maps the names of temporary partition tables to their final table names.
	tableTemps := make(map[string]string)
	// Process MARC data
	inputCount, writeCount, err := process(opts, marct, dbc, printerr, tableTemps)
	if err != nil {
		return err
	}
	// Index columns
	if err = index(marct, dbc, printerr); err != nil {
		return err
	}
	// Install tables
	if err = install(marct, dbc, tableTemps, printerr); err != nil {
		return err
	}
	// Grant permission to LDP user
	for _, u := range marct.Users {
		if err = grant(marct, dbc, u); err != nil {
			return err
		}
	}
	if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS dbsystem.ldpmarc_cksum"); err != nil {
		return fmt.Errorf("dropping table \"dbsystem.ldpmarc_cksum\": %v", err)
	}
	if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS dbsystem.ldpmarc_metadata"); err != nil {
		return fmt.Errorf("dropping table \"dbsystem.ldpmarc_metadata\": %v", err)
	}
	if inputCount > 0 {
		startCksum := time.Now()
		if err = inc.CreateCksum(dbc, marct.Loc.SrsRecords, marct.Loc.SrsMarc, marct.Loc.tablefinal(),
			marct.Loc.SrsMarcAttr); err != nil {
			return err
		}
		if marct.Verbose >= 1 {
			printerr(" %s checksum", util.ElapsedTime(startCksum))
		}
		/*
			startVacuum := time.Now()
			if err = util.Vacuum(context.TODO(), dbc, marct.Loc.tablefinal()); err != nil {
				return err
			}
			if err = inc.VacuumCksum(context.TODO(), dbc); err != nil {
				return err
			}
			if marct.Verbose >= 1 {
				printerr(" %s vacuum", util.ElapsedTime(startVacuum))
			}
		*/
	}
	if marct.Verbose >= 1 {
		printerr("%s full update", util.ElapsedTime(startUpdate))
		printerr("%d output rows", writeCount)
		printerr("new table is ready to use: " + marct.Loc.tablefinal())
	}
	return nil
}

func process(opts *options.Options, marct *MARCTransform, dbc *util.DBC, printerr PrintErr, tableTemps map[string]string) (int64, int64, error) {
	var err error
	var store *local.Store
	if store, err = local.NewStore(marct.Datadir); err != nil {
		return 0, 0, err
	}
	defer store.Close()
	if err = setupTables(dbc, tableTemps); err != nil {
		return 0, 0, err
	}

	var inputCount, writeCount int64
	if inputCount, err = selectCount(dbc, marct.Loc.SrsRecords); err != nil {
		return 0, 0, err
	}
	if marct.Verbose >= 1 {
		printerr("%d input rows", inputCount)
	}
	// main processing
	if inputCount > 0 {
		if writeCount, err = processAll(opts, marct, dbc, store, printerr, tableTemps); err != nil {
			return 0, 0, err
		}
	}
	return inputCount, writeCount, nil
}

func setupTables(dbc *util.DBC, tableTemps map[string]string) error {
	var err error
	var q string
	if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout); err != nil {
		return fmt.Errorf("dropping table %q: %v", tableout, err)
	}
	var lz4 string
	if util.IsLZ4Available(dbc) {
		lz4 = " COMPRESSION lz4"
	}
	q = "" +
		"CREATE TABLE " + tableout + " (" +
		"    srs_id uuid NOT NULL," +
		"    line smallint NOT NULL," +
		"    matched_id uuid NOT NULL," +
		"    instance_hrid varchar(32) NOT NULL," +
		"    instance_id uuid NOT NULL," +
		"    field varchar(3) NOT NULL," +
		"    ind1 varchar(1) NOT NULL," +
		"    ind2 varchar(1) NOT NULL," +
		"    ord smallint NOT NULL," +
		"    sf varchar(1) NOT NULL," +
		"    content varchar(65535)" + lz4 + " NOT NULL" +
		") PARTITION BY LIST (field);"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table: %s", err)
	}
	q = "COMMENT ON TABLE " + tableout + " IS 'current MARC records in tabular form'"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("adding comment on table: %s", err)
	}
	for _, field := range allFields {
		if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS ldpmarc.srs_marctab_"+field); err != nil {
			return fmt.Errorf("dropping table \"ldpmarc.srs_marctab_%s\": %v", field, err)
		}
		if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout+field); err != nil {
			return fmt.Errorf("dropping table %q: %v", tableout+field, err)
		}
		q = "CREATE TABLE " + tableout + field +
			" PARTITION OF " + tableout + " FOR VALUES IN ('" + field + "')" +
			" PARTITION BY LIST (sf)"
		if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("creating field partition: %s", err)
		}
		tableTemps[tableoutTable+field] = "mt" + field
	}
	if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS ldpmarc.cksum"); err != nil {
		return fmt.Errorf("dropping table \"ldpmarc.cksum\": %v", err)
	}
	if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS ldpmarc.metadata"); err != nil {
		return fmt.Errorf("dropping table \"ldpmarc.metadata\": %v", err)
	}
	if _, err = dbc.Conn.Exec(context.TODO(), "DROP SCHEMA IF EXISTS ldpmarc"); err != nil {
		return fmt.Errorf("dropping schema \"ldpmarc\": %v", err)
	}
	return nil
}

func selectCount(dbc *util.DBC, tablein string) (int64, error) {
	var err error
	var count int64
	var q = "SELECT count(*) FROM " + tablein + ";"
	if err = dbc.Conn.QueryRow(context.TODO(), q).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func processAll(opts *options.Options, marct *MARCTransform, dbc *util.DBC, store *local.Store, printerr PrintErr,
	tableTemps map[string]string) (int64, error) {

	startTime := time.Now()

	var err error
	var msg *string
	var writeCount int64
	sfmap := make(map[util.FieldSF]struct{})

	var q = "SELECT r.id, r.matched_id, r.external_hrid instance_hrid, m." + marct.Loc.SrsMarcAttr +
		"::text FROM " + marct.Loc.SrsRecords + " r JOIN " + marct.Loc.SrsMarc + " m ON r.id = m.id" +
		" WHERE r.state = 'ACTUAL'"
	state := "ACTUAL"
	var rows pgx.Rows
	if rows, err = dbc.Conn.Query(context.TODO(), q); err != nil {
		return 0, fmt.Errorf("selecting marc records: %v", err)
	}
	for rows.Next() {
		var id, matchedID, instanceHRID, data *string
		if err = rows.Scan(&id, &matchedID, &instanceHRID, &data); err != nil {
			return 0, fmt.Errorf("scanning records: %v", err)
		}
		var record local.Record
		var instanceID string
		var mrecs []marc.Marc
		var skip bool
		id, matchedID, instanceHRID, instanceID, mrecs, skip = util.Transform(id, matchedID, instanceHRID,
			&state, data, printerr, marct.Verbose)
		if skip {
			continue
		}
		var m marc.Marc
		for _, m = range mrecs {
			record.SRSID = *id
			record.Line = m.Line
			record.MatchedID = *matchedID
			record.InstanceHRID = *instanceHRID
			record.InstanceID = instanceID
			record.Field = m.Field
			record.Ind1 = m.Ind1
			record.Ind2 = m.Ind2
			record.Ord = m.Ord
			record.SF = m.SF
			record.Content = m.Content
			msg, err = store.Write(&record)
			if err != nil {
				return 0, fmt.Errorf("writing record: %v: %v", err, record)
			}
			if msg != nil {
				printerr("skipping line in record: %s: %s", *id, *msg)
				continue
			}
			fieldSF := util.FieldSF{Field: m.Field, SF: m.SF}
			_, ok := sfmap[fieldSF]
			if !ok {
				sfmap[fieldSF] = struct{}{}
			}
			writeCount++
		}
	}
	if rows.Err() != nil {
		return 0, fmt.Errorf("row error: %v", rows.Err())
	}
	rows.Close()

	if err = store.FinishWriting(); err != nil {
		return 0, err
	}

	if err = createSFPartitions(opts, dbc, sfmap, tableTemps); err != nil {
		return 0, err
	}

	if marct.Verbose >= 1 {
		printerr(" %s transform", util.ElapsedTime(startTime))
	}

	startTime = time.Now()

	var f string
	for _, f = range allFields {
		src, err := store.ReadSource(f, printerr)
		if err != nil {
			return 0, err
		}
		_, err = dbc.Conn.CopyFrom(context.TODO(),
			pgx.Identifier{tableoutSchema, tableoutTable + f},
			[]string{"srs_id", "line", "matched_id", "instance_hrid", "instance_id", "field", "ind1", "ind2", "ord", "sf", "content"},
			src)
		if err != nil {
			return 0, fmt.Errorf("copying to database: %v", err)
		}
		src.Close()
	}

	if marct.Verbose >= 1 {
		printerr(" %s load", util.ElapsedTime(startTime))
	}

	return writeCount, nil
}

func createSFPartitions(opts *options.Options, dbc *util.DBC, sfmap map[util.FieldSF]struct{},
	tableTemps map[string]string) error {
	for f := range sfmap {
		t := opts.SFPartitionTable(f.Field, f.SF)
		q := "CREATE TABLE " + opts.TempPartitionSchema + "." + opts.TempTablePrefix + t +
			" PARTITION OF " +
			opts.TempPartitionSchema + "." + opts.TempTablePrefix + opts.PartitionTableBase + f.Field +
			" FOR VALUES IN ('" + util.EscapeSFString(f.SF) + "')"
		if _, err := dbc.Conn.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("creating sf partition: %s", err)
		}
		tableTemps[opts.TempTablePrefix+t] = t
	}
	return nil
}

func index(opts *MARCTransform, dbc *util.DBC, printerr PrintErr) error {
	startIndex := time.Now()
	var err error
	// Index columns
	var cols = []string{"srs_id", "instance_hrid"}
	if err = indexColumns(opts, dbc, cols, printerr); err != nil {
		return err
	}
	if opts.Verbose >= 1 {
		printerr(" %s index", util.ElapsedTime(startIndex))
	}
	return nil
}

func indexColumns(opts *MARCTransform, dbc *util.DBC, cols []string, printerr PrintErr) error {
	for _, c := range cols {
		if opts.Verbose >= 2 {
			printerr("creating index: %s", c)
		}
		var q = "CREATE INDEX ON " + tableout + " (" + c + ")"
		if _, err := dbc.Conn.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("creating index: %s: %s", c, err)
		}
	}
	return nil
}

func install(opts *MARCTransform, dbc *util.DBC, tableTemps map[string]string, printerr PrintErr) error {
	start := time.Now()

	// Clean up obsolete table.
	if _, err := dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS folio_source_record.marctab"); err != nil {
		return fmt.Errorf("dropping table \"folio_source_record.marctab\": %v", err)
	}

	tx, err := util.BeginTx(context.TODO(), dbc.Conn)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.TODO())

	q := "DROP TABLE IF EXISTS " + tableoutSchema + "." + opts.Loc.TablefinalTable
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "ALTER TABLE " + tableout + " RENAME TO " + opts.Loc.TablefinalTable
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("renaming table: %s", err)
	}
	q = "DROP TABLE IF EXISTS " + opts.Loc.tablefinal()
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "ALTER TABLE " + tableoutSchema + "." + opts.Loc.TablefinalTable + " SET SCHEMA " + opts.Loc.TablefinalSchema
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("moving table: %s", err)
	}
	for oldT, newT := range tableTemps {
		q = "DROP TABLE IF EXISTS " + tableoutSchema + "." + newT
		if _, err = tx.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("dropping table: %s", err)
		}
		q = "ALTER TABLE " + tableoutSchema + "." + oldT + " RENAME TO " + newT
		if _, err = tx.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("renaming table: %s", err)
		}
	}
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	if opts.Verbose >= 1 {
		printerr(" %s install", util.ElapsedTime(start))
	}
	return nil
}

func grant(opts *MARCTransform, dbc *util.DBC, user string) error {
	var err error
	// Grant permission to LDP user
	var q = "GRANT USAGE ON SCHEMA " + opts.Loc.TablefinalSchema + " TO " + user
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("schema permission: %s", err)
	}
	q = "GRANT SELECT ON " + opts.Loc.tablefinal() + " TO " + user
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("table permission: %s", err)
	}
	return nil
}

func (l Locations) tablefinal() string {
	return l.TablefinalSchema + "." + l.TablefinalTable
}

func readConfigMetadb(opts *MARCTransform) (string, string, string, string, string, string, error) {
	var mdbconf = filepath.Join(opts.Datadir, "metadb.conf")
	cfg, err := ini.Load(mdbconf)
	if err != nil {
		return "", "", "", "", "", "", nil
	}
	s := cfg.Section("main")
	host := s.Key("host").String()
	port := s.Key("port").String()
	user := s.Key("systemuser").String()
	password := s.Key("systemuser_password").String()
	dbname := s.Key("database").String()
	sslmode := s.Key("sslmode").String()
	return host, port, user, password, dbname, sslmode, nil
}

func readConfigLDP1(opts *MARCTransform) (string, string, string, string, string, string, error) {
	var ldpconf = filepath.Join(opts.Datadir, "ldpconf.json")
	viper.SetConfigFile(ldpconf)
	viper.SetConfigType("json")
	var ok bool
	if err := viper.ReadInConfig(); err != nil {
		if _, ok = err.(viper.ConfigFileNotFoundError); ok {
			return "", "", "", "", "", "", fmt.Errorf("file not found: %s", ldpconf)
		} else {
			return "", "", "", "", "", "", fmt.Errorf("error reading file: %s: %s", ldpconf, err)
		}
	}
	var ldp = "ldp_database"
	var host = viper.GetString(ldp + ".database_host")
	var port = strconv.Itoa(viper.GetInt(ldp + ".database_port"))
	var user = viper.GetString(ldp + ".database_user")
	var password = viper.GetString(ldp + ".database_password")
	var dbname = viper.GetString(ldp + ".database_name")
	var sslmode = viper.GetString(ldp + ".database_sslmode")
	return host, port, user, password, dbname, sslmode, nil
}
