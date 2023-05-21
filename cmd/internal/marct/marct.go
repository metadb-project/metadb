package marct

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/internal/marct/inc"
	"github.com/metadb-project/metadb/cmd/internal/marct/local"
	"github.com/metadb-project/metadb/cmd/internal/marct/marc"
	"github.com/metadb-project/metadb/cmd/internal/marct/util"
	"github.com/spf13/viper"
	"gopkg.in/ini.v1"
)

type TransformOptions struct {
	FullUpdate   bool
	Datadir      string
	Users        []string
	TrigramIndex bool
	NoIndexes    bool
	Verbose      int // 0=quiet, 1=summary, 2=detail
	CSVFileName  string
	SRSRecords   string
	SRSMarc      string
	SRSMarcAttr  string
	Metadb       bool
	PrintErr     PrintErr
	Loc          Locations
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

var csvFile *os.File

/*
func main() {
	flag.Parse()
	if len(flag.Args()) > 0 {
		printerr("invalid argument: %s", flag.Arg(0))
		os.Exit(2)
	}
	if *helpFlag || *datadirFlag == "" {
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %s:\n", program)
		flag.PrintDefaults()
		if *helpFlag {
			return
		} else {
			os.Exit(2)
		}
	}
	if *incUpdateFlag {
		printerr("-i option no longer supported")
		os.Exit(1)
	}
	if *noTrigramIndexFlag {
		printerr("-T option no longer supported")
		os.Exit(1)
	}
	loc := setupLocations()
	if err := run(loc); err != nil {
		printerr("%s", err)
		os.Exit(1)
	}
}
*/

func Run(opts *TransformOptions) error {
	opts.Loc = setupLocations(opts)
	// Read database configuration
	var host, port, user, password, dbname, sslmode string
	var err error
	if opts.Metadb {
		host, port, user, password, dbname, sslmode, err = readConfigMetadb(opts)
		if err != nil {
			return err
		}
	} else {
		host, port, user, password, dbname, sslmode, err = readConfigLDP1(opts)
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
		if !retry && incUpdateAvail && !opts.FullUpdate && opts.CSVFileName == "" {
			if opts.Verbose >= 1 {
				opts.PrintErr("starting incremental update")
			}
			err = inc.IncrementalUpdate(connString, opts.Loc.SrsRecords, opts.Loc.SrsMarc, opts.Loc.SrsMarcAttr,
				opts.Loc.tablefinal(), opts.PrintErr, opts.Verbose)
			if err != nil {
				opts.PrintErr("restarting with full update due to early termination: %v", err)
				retry = true
			}
		} else {
			retry = false
			if opts.Verbose >= 1 {
				opts.PrintErr("starting full update")
			}
			if err = fullUpdate(opts, connString, opts.PrintErr); err != nil {
				_, _ = conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout)
				return err
			}
		}
		if !retry {
			break
		}
	}
	return nil
}

func setupLocations(opts *TransformOptions) Locations {
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

func fullUpdate(opts *TransformOptions, connString string, printerr PrintErr) error {
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
	// Vacuum in case previous run was not completed.
	// _ = util.Vacuum(context.TODO(), dbc, opts.Loc.tablefinal())
	// _ = inc.VacuumCksum(context.TODO(), dbc)
	if opts.CSVFileName != "" {
		if csvFile, err = os.Create(opts.CSVFileName); err != nil {
			return err
		}
		defer func(csvFile *os.File) {
			_ = csvFile.Close()
		}(csvFile)
		if opts.Verbose >= 1 {
			printerr("output will be written to file: %s", opts.CSVFileName)
		}
	}
	// Process MARC data
	inputCount, writeCount, err := process(opts, dbc, printerr)
	if err != nil {
		return err
	}
	if opts.CSVFileName == "" {
		// Index columns
		if !opts.NoIndexes {
			if err = index(opts, dbc, printerr); err != nil {
				return err
			}
		}
		// Replace table
		if err = replace(opts, dbc); err != nil {
			return err
		}
		// Grant permission to LDP user
		for _, u := range opts.Users {
			if err = grant(opts, dbc, u); err != nil {
				return err
			}
		}
		_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS dbsystem.ldpmarc_cksum;")
		_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS dbsystem.ldpmarc_metadata;")
		if inputCount > 0 {
			startCksum := time.Now()
			if err = inc.CreateCksum(dbc, opts.Loc.SrsRecords, opts.Loc.SrsMarc, opts.Loc.tablefinal(),
				opts.Loc.SrsMarcAttr); err != nil {
				return err
			}
			if opts.Verbose >= 1 {
				printerr(" %s checksum", util.ElapsedTime(startCksum))
			}
			/*
				startVacuum := time.Now()
				if err = util.Vacuum(context.TODO(), dbc, opts.Loc.tablefinal()); err != nil {
					return err
				}
				if err = inc.VacuumCksum(context.TODO(), dbc); err != nil {
					return err
				}
				if opts.Verbose >= 1 {
					printerr(" %s vacuum", util.ElapsedTime(startVacuum))
				}
			*/
		}
		if opts.Verbose >= 1 {
			printerr("%s full update", util.ElapsedTime(startUpdate))
			printerr("%d output rows", writeCount)
			printerr("new table is ready to use: " + opts.Loc.tablefinal())
		} else {

		}
	}
	return nil
}

func process(opts *TransformOptions, dbc *util.DBC, printerr PrintErr) (int64, int64, error) {
	var err error
	var store *local.Store
	if store, err = local.NewStore(opts.Datadir); err != nil {
		return 0, 0, err
	}
	defer store.Close()
	if err = setupTables(opts, dbc); err != nil {
		return 0, 0, err
	}

	var inputCount, writeCount int64
	if inputCount, err = selectCount(dbc, opts.Loc.SrsRecords); err != nil {
		return 0, 0, err
	}
	if opts.Verbose >= 1 {
		printerr("%d input rows", inputCount)
	}
	// main processing
	if inputCount > 0 {
		if writeCount, err = processAll(opts, dbc, store, printerr); err != nil {
			return 0, 0, err
		}
	}
	return inputCount, writeCount, nil
}

func setupTables(opts *TransformOptions, dbc *util.DBC) error {
	var err error
	var q string
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout)
	if opts.TrigramIndex && !util.IsTrgmAvailable(dbc) {
		return fmt.Errorf("unable to access pg_trgm module extension")
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
		_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS ldpmarc.srs_marctab_"+field)
		_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout+field)
		q = "CREATE TABLE " + tableout + field +
			" PARTITION OF " + tableout + " FOR VALUES IN ('" + field + "')"
		if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("creating partition: %s", err)
		}
	}
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS ldpmarc.cksum")
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS ldpmarc.metadata")
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP SCHEMA IF EXISTS ldpmarc")
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

func processAll(opts *TransformOptions, dbc *util.DBC, store *local.Store, printerr PrintErr) (int64, error) {
	startTime := time.Now()

	var err error
	var msg *string
	var writeCount int64
	var q = "SELECT r.id, r.matched_id, r.external_hrid instance_hrid, m." + opts.Loc.SrsMarcAttr +
		"::text FROM " + opts.Loc.SrsRecords + " r JOIN " + opts.Loc.SrsMarc + " m ON r.id = m.id" +
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
			&state, data, printerr, opts.Verbose)
		if skip {
			continue
		}
		var m marc.Marc
		for _, m = range mrecs {
			if opts.CSVFileName == "" {
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
				writeCount++
			} else {
				_, _ = fmt.Fprintf(csvFile, "%q,%d,%q,%q,%q,%q,%q,%q,%d,%q,%q\n", *id, m.Line, *matchedID, *instanceHRID, instanceID, m.Field, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content)
				writeCount++
			}
		}
	}
	if rows.Err() != nil {
		return 0, fmt.Errorf("row error: %v", rows.Err())
	}
	rows.Close()

	if err = store.FinishWriting(); err != nil {
		return 0, err
	}

	if opts.Verbose >= 1 {
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

	if opts.Verbose >= 1 {
		printerr(" %s load", util.ElapsedTime(startTime))
	}

	return writeCount, nil
}

func index(opts *TransformOptions, dbc *util.DBC, printerr PrintErr) error {
	startIndex := time.Now()
	var err error
	// Index columns
	var cols = []string{
		"srs_id",
		"matched_id",
		"instance_hrid",
		"instance_id",
		"sf"}
	if opts.TrigramIndex {
		cols = append(cols, "content")
	}
	if err = indexColumns(opts, dbc, cols, printerr); err != nil {
		return err
	}
	if opts.Verbose >= 1 {
		printerr(" %s index", util.ElapsedTime(startIndex))
	}
	return nil
}

func indexColumns(opts *TransformOptions, dbc *util.DBC, cols []string, printerr PrintErr) error {
	for _, c := range cols {
		if opts.Verbose >= 2 {
			printerr("creating index: %s", c)
		}
		if c == "content" {
			var q = "CREATE INDEX ON " + tableout + " USING GIN (" + c + " gin_trgm_ops)"
			if _, err := dbc.Conn.Exec(context.TODO(), q); err != nil {
				return fmt.Errorf("creating index with pg_trgm extension: %s: %s", c, err)
			}
		} else {
			var q = "CREATE INDEX ON " + tableout + " (" + c + ")"
			if _, err := dbc.Conn.Exec(context.TODO(), q); err != nil {
				return fmt.Errorf("creating index: %s: %s", c, err)
			}
		}
	}
	return nil
}

func replace(opts *TransformOptions, dbc *util.DBC) error {
	// Clean up old tables
	q := "DROP TABLE IF EXISTS folio_source_record.marctab"
	_, err := dbc.Conn.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "DROP TABLE IF EXISTS public.srs_marctab"
	_, err = dbc.Conn.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}

	q = "DROP TABLE IF EXISTS " + tableoutSchema + "." + opts.Loc.TablefinalTable
	_, err = dbc.Conn.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "ALTER TABLE " + tableout + " RENAME TO " + opts.Loc.TablefinalTable
	_, err = dbc.Conn.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("renaming table: %s", err)
	}
	q = "DROP TABLE IF EXISTS " + opts.Loc.tablefinal()
	_, err = dbc.Conn.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "ALTER TABLE " + tableoutSchema + "." + opts.Loc.TablefinalTable + " SET SCHEMA " + opts.Loc.TablefinalSchema
	_, err = dbc.Conn.Exec(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("moving table: %s", err)
	}
	for _, field := range allFields {
		q = "DROP TABLE IF EXISTS " + tableoutSchema + "." + opts.Loc.TablefinalTable + field
		_, err = dbc.Conn.Exec(context.TODO(), q)
		if err != nil {
			return fmt.Errorf("dropping table: %s", err)
		}
		q = "ALTER TABLE " + tableout + field + " RENAME TO mt" + field
		_, err = dbc.Conn.Exec(context.TODO(), q)
		if err != nil {
			return fmt.Errorf("renaming table: %s", err)
		}
	}
	return nil
}

func grant(opts *TransformOptions, dbc *util.DBC, user string) error {
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

/*
func setupLocations() *locations {
	loc := &locations{
		SrsRecords:       "folio_source_record.records_lb",
		SrsMarc:          "folio_source_record.marc_records_lb",
		SrsMarcAttr:      "content",
		TablefinalSchema: "folio_source_record",
		TablefinalTable:  "marctab",
	}
	if !*metadbFlag { // LDP1
		loc.SrsRecords = "public.srs_records"
		loc.SrsMarc = "public.srs_marc"
		loc.SrsMarcAttr = "data"
		loc.TablefinalSchema = "public"
		loc.TablefinalTable = "srs_marctab"
	}
	if *srsRecordsFlag != "" {
		loc.SrsRecords = *srsRecordsFlag
	}
	if *srsMarcFlag != "" {
		loc.SrsMarc = *srsMarcFlag
	}
	if *srsMarcAttrFlag != "" {
		loc.SrsMarcAttr = *srsMarcAttrFlag
	}
	return loc
}

type locations struct {
	SrsRecords       string
	SrsMarc          string
	SrsMarcAttr      string
	TablefinalSchema string
	TablefinalTable  string
}
*/

func (l Locations) tablefinal() string {
	return l.TablefinalSchema + "." + l.TablefinalTable
}

func readConfigMetadb(opts *TransformOptions) (string, string, string, string, string, string, error) {
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

func readConfigLDP1(opts *TransformOptions) (string, string, string, string, string, string, error) {
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

//func printerr(format string, v ...any) {
//	_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", "LDPMARC", fmt.Sprintf(format, v...))
//}
