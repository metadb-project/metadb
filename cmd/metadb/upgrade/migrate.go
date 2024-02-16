package upgrade

import (
	"bufio"
	"cmp"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nazgaret/metadb/cmd/internal/eout"
	"github.com/nazgaret/metadb/cmd/internal/uuid"
	"github.com/nazgaret/metadb/cmd/metadb/catalog"
	"github.com/nazgaret/metadb/cmd/metadb/command"
	"github.com/nazgaret/metadb/cmd/metadb/dbx"
	"github.com/nazgaret/metadb/cmd/metadb/option"
	"github.com/nazgaret/metadb/cmd/metadb/process"
	"github.com/nazgaret/metadb/cmd/metadb/util"
	"github.com/spf13/viper"
)

func Migrate(opt *option.Migrate) error {
	// Ask for confirmation
	_, _ = fmt.Fprintf(os.Stderr, "Begin migration process? ")
	var confirm string
	_, err := fmt.Scanln(&confirm)
	if err != nil || (confirm != "y" && confirm != "Y" && strings.ToUpper(confirm) != "YES") {
		return nil
	}

	// Check if server is already running.
	running, pid, err := process.IsServerRunning(opt.Datadir)
	if err != nil {
		return err
	}
	if running {
		return fmt.Errorf("lock file %q already exists and server (PID %d) appears to be running",
			util.SystemPIDFileName(opt.Datadir), pid)
	}
	// Write lock file for new server instance.
	if err = process.WritePIDFile(opt.Datadir); err != nil {
		return err
	}
	defer process.RemovePIDFile(opt.Datadir)

	db, err := util.ReadConfigDatabase(opt.Datadir)
	if err != nil {
		return err
	}
	dp, err := dbx.NewPool(context.TODO(), db.ConnString(db.User, db.Password))
	if err != nil {
		return fmt.Errorf("creating Metadb database connection pool: %v", err)
	}
	defer dp.Close()

	// Check that database version is compatible.
	if err = catalog.CheckDatabaseCompatible(dp); err != nil {
		return err
	}

	// Start catalog which we can use to create tables.
	cat, err := catalog.Initialize(db, dp)
	if err != nil {
		return err
	}

	dbLDP, err := readConfigLDP(opt.LDPConf)
	if err != nil {
		return err
	}
	dpLDP, err := dbx.NewPool(context.TODO(), dbLDP.ConnString(dbLDP.User, dbLDP.Password))
	if err != nil {
		return fmt.Errorf("creating LDP database connection pool: %v", err)
	}
	defer dpLDP.Close()

	q := "SELECT database_version FROM dbsystem.main"
	var i int64
	err = dpLDP.QueryRow(context.TODO(), q).Scan(&i)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		fallthrough
	case err != nil:
		return fmt.Errorf("invalid LDP database: %v", err)
	default:
		// NOP
	}

	basepath := filepath.Join(opt.Datadir, "tmp/ldp_migration")
	_ = os.RemoveAll(basepath)
	if err = os.MkdirAll(basepath, 0700); err != nil {
		return fmt.Errorf("unable to make directory: %v: %v", basepath, err)
	}
	defer os.RemoveAll(basepath)

	if err = runMigration(dp, cat, dpLDP, opt.Source, basepath); err != nil {
		return err
	}
	eout.Info("migration complete")
	return nil
}

func runMigration(dp *pgxpool.Pool, cat *catalog.Catalog, dpLDP *pgxpool.Pool, source string, basepath string) error {
	tmap := getLDPTableMap()
	for _, m := range tmap {
		q := fmt.Sprintf("SELECT id FROM history.%s LIMIT 1", m.ldpTable)
		var id string
		err := dpLDP.QueryRow(context.TODO(), q).Scan(&id)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// NOP
		case err != nil:
			eout.Info("table \"history.%s\" not found", m.ldpTable)
			continue
		default:
			// NOP
		}
		// Ensure the Metadb table exists.
		table, _ := dbx.ParseTable(m.metadbTable)
		if !cat.TableExists(&table) {
			eout.Info("creating table %s__", m.metadbTable)
			if err = cat.CreateNewTable(&table, false, &dbx.Table{}, source); err != nil {
				return fmt.Errorf("creating table \"%s__\": %v", table, err)
			}
		}
		c := &dbx.Column{Schema: table.Schema, Table: table.Table, Column: "id"}
		if cat.Column(c) == nil {
			if err = cat.AddColumn(&table, "id", command.UUIDType, 0); err != nil {
				return fmt.Errorf("adding column \"id\" in table \"%s__\": %v", table, err)
			}
		}
		c = &dbx.Column{Schema: table.Schema, Table: table.Table, Column: m.jsonColumn}
		if cat.Column(c) == nil {
			if err = cat.AddColumn(&table, m.jsonColumn, command.JSONType, 0); err != nil {
				return fmt.Errorf("adding column %q in table \"%s__\": %v", m.jsonColumn, table, err)
			}
		}
		// Get the current time from the database, to use as a consistent cut-off time.
		now, err := selectNow(dp)
		if err != nil {
			return fmt.Errorf("querying current time: %v", err)
		}
		// Find minimum start time.
		minStart, err := selectMinStart(dp, m.metadbTable, now)
		if err != nil {
			return fmt.Errorf("querying minimum start: %v", err)
		}
		eout.Info("migrating: %s__: reading history.%s where (updated < %s)", m.metadbTable, m.ldpTable, minStart)
		path := filepath.Join(basepath, m.ldpTable)
		if err := queueMigrationTable(dpLDP, cat, m, table, minStart, path); err != nil {
			return err
		}
		if err := copyMigrationTable(dp, m, path); err != nil {
			return err
		}
		os.Remove(path)
	}
	return nil
}

func copyMigrationTable(dp *pgxpool.Pool, tm ldpTableMap, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("unable to open file for reading: %v: %v", path, err)
	}
	defer file.Close()
	src := &migrationSource{
		decoder: gob.NewDecoder(bufio.NewReader(file)),
		path:    path,
	}

	table, _ := dbx.ParseTable(tm.metadbTable)
	copyCount, err := dp.CopyFrom(context.TODO(),
		pgx.Identifier{table.Schema, table.Table + "__"},
		[]string{"__start", "__end", "__current", "__origin", "id", "jsonb"},
		src)
	if err != nil {
		return fmt.Errorf("copying to database: %v", err)
	}
	eout.Info("migrating: %s__: %d records written", tm.metadbTable, copyCount)
	return nil
}

type migrationSource struct {
	record  *migrationRecord
	decoder *gob.Decoder
	path    string
	err     error
}

func (s *migrationSource) Next() bool {
	var record = new(migrationRecord)
	err := s.decoder.Decode(record)
	switch {
	case err == io.EOF:
		return false
	case err != nil:
		s.err = err
		return false
	default:
		s.record = record
		return true
	}
}

func (s *migrationSource) Values() ([]any, error) {
	switch {
	case s.err != nil:
		return nil, s.err
	case s.record == nil:
		s.err = fmt.Errorf("no record available: %s", s.path)
		return nil, s.err
	default:
		id, err := uuid.EncodeUUID(s.record.ID)
		if err != nil {
			return nil, fmt.Errorf("encoding ID: %v", err)
		}
		var v = []any{
			s.record.Start, // __start
			s.record.End,   // __end
			false,          // __current
			"ldp",          // __origin
			id,             // id
			s.record.JSONB, // jsonb
		}
		return v, nil
	}
}

func (s *migrationSource) Err() error {
	return s.err
}

func queueMigrationTable(dpLDP *pgxpool.Pool, cat *catalog.Catalog, tm ldpTableMap, metadbTable dbx.Table, minStart time.Time, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("unable to create file: %v: %v", path, err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)

	q := fmt.Sprintf(
		"SELECT id::text, data::text, updated FROM history.%s WHERE updated < $1 ORDER BY id, updated",
		tm.ldpTable)
	rows, err := dpLDP.Query(context.TODO(), q, minStart)
	if err != nil {
		return fmt.Errorf("selecting historical data: %v", err)
	}
	defer rows.Close()
	var prevRecord, nextRecord migrationRecord
	var prevID string
	firstRow := true
	for rows.Next() {
		var id, data string
		var up time.Time
		if err = rows.Scan(&id, &data, &up); err != nil {
			return fmt.Errorf("reading historical records: %v", err)
		}
		updated := up.UTC()
		if id == prevID {
			prevRecord.End = updated
		} else {
			prevRecord.End = minStart
		}
		nextRecord.Start = updated
		nextRecord.ID = id
		nextRecord.JSONB = data
		if firstRow {
			firstRow = false
		} else {
			if err = queueMigrationRecord(cat, metadbTable, encoder, &prevRecord); err != nil {
				return err
			}
		}
		prevRecord = nextRecord
		prevID = id
	}
	prevRecord.End = minStart
	if !firstRow {
		if err = queueMigrationRecord(cat, metadbTable, encoder, &prevRecord); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("reading text columns: %v", err)
	}
	if err = writer.Flush(); err != nil {
		return fmt.Errorf("flushing buffer for file: %v: %v", path, err)
	}
	return nil
}

func queueMigrationRecord(cat *catalog.Catalog, metadbTable dbx.Table, encoder *gob.Encoder, record *migrationRecord) error {
	if err := encoder.Encode(*record); err != nil {
		return fmt.Errorf("encoding record: %v: %v", err, *record)
	}
	if err := makeRecordPartition(cat, metadbTable, record.Start); err != nil {
		return fmt.Errorf("making partition for record: %v: %v", err, *record)
	}
	return nil
}

func makeRecordPartition(cat *catalog.Catalog, metadbTable dbx.Table, timestamp time.Time) error {
	year := timestamp.Year()
	if cat.PartYearExists(metadbTable.Schema, metadbTable.Table, year) {
		return nil
	}
	if err := cat.AddPartYear(metadbTable.Schema, metadbTable.Table, year); err != nil {
		return fmt.Errorf("adding partition for table %q year %d: %v", metadbTable.Main().String(),
			year, err)
	}
	return nil
}

type migrationRecord struct {
	Start time.Time
	End   time.Time
	ID    string
	JSONB string
}

func selectNow(dp *pgxpool.Pool) (time.Time, error) {
	q := "SELECT now()"
	var now time.Time
	err := dp.QueryRow(context.TODO(), q).Scan(&now)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return time.Time{}, err
	case err != nil:
		return time.Time{}, err
	default:
		return now.UTC(), nil
	}
}

func selectMinStart(dp *pgxpool.Pool, table string, now time.Time) (time.Time, error) {
	q := fmt.Sprintf("SELECT min(__start) FROM %s__", table)
	var minStart *time.Time
	err := dp.QueryRow(context.TODO(), q).Scan(&minStart)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return now, nil
	case err != nil:
		return time.Time{}, err
	default:
		if minStart == nil {
			return now, nil
		} else {
			return minStart.UTC(), nil
		}
	}
}

func readConfigLDP(ldpconf string) (*dbx.DB, error) {
	viper.SetConfigFile(ldpconf)
	viper.SetConfigType("json")
	var ok bool
	if err := viper.ReadInConfig(); err != nil {
		if _, ok = err.(viper.ConfigFileNotFoundError); ok {
			return nil, fmt.Errorf("file not found: %s", ldpconf)
		} else {
			return nil, fmt.Errorf("error reading file: %s: %s", ldpconf, err)
		}
	}
	ldp := "ldp_database"
	return &dbx.DB{
		Host:          viper.GetString(ldp + ".database_host"),
		Port:          strconv.Itoa(viper.GetInt(ldp + ".database_port")),
		User:          viper.GetString(ldp + ".database_user"),
		Password:      viper.GetString(ldp + ".database_password"),
		SuperUser:     "",
		SuperPassword: "",
		DBName:        viper.GetString(ldp + ".database_name"),
		SSLMode:       viper.GetString(ldp + ".database_sslmode"),
	}, nil
}

type ldpTableMap struct {
	ldpTable    string
	metadbTable string
	jsonColumn  string
}

func getLDPTableMap() []ldpTableMap {
	m := []ldpTableMap{
		{"acquisition_method", "folio_orders.acquisition_method", "jsonb"},
		{"acquisitions_memberships", "folio_orders.acquisitions_unit_membership", "jsonb"},
		{"acquisitions_units", "folio_orders.acquisitions_unit", "jsonb"},
		{"audit_circulation_logs", "folio_audit.circulation_logs", "jsonb"},
		{"circulation_cancellation_reasons", "folio_circulation.cancellation_reason", "jsonb"},
		{"circulation_check_ins", "folio_circulation.check_in", "jsonb"},
		{"circulation_fixed_due_date_schedules", "folio_circulation.fixed_due_date_schedule", "jsonb"},
		{"circulation_loan_history", "folio_circulation.audit_loan", "jsonb"},
		{"circulation_loan_policies", "folio_circulation.loan_policy", "jsonb"},
		{"circulation_loans", "folio_circulation.loan", "jsonb"},
		{"circulation_patron_action_sessions", "folio_circulation.patron_action_session", "jsonb"},
		{"circulation_patron_notice_policies", "folio_circulation.patron_notice_policy", "jsonb"},
		{"circulation_request_policies", "folio_circulation.request_policy", "jsonb"},
		{"circulation_request_preference", "folio_circulation.user_request_preference", "jsonb"},
		{"circulation_requests", "folio_circulation.request", "jsonb"},
		{"circulation_scheduled_notices", "folio_circulation.scheduled_notice", "jsonb"},
		{"circulation_staff_slips", "folio_circulation.staff_slips", "jsonb"},
		{"configuration_entries", "folio_configuration.config_data", "jsonb"},
		{"course_copyrightstatuses", "folio_courses.coursereserves_copyrightstates", "jsonb"},
		{"course_courselistings", "folio_courses.coursereserves_courselistings", "jsonb"},
		{"course_courses", "folio_courses.coursereserves_courses", "jsonb"},
		{"course_coursetypes", "folio_courses.coursereserves_coursetypes", "jsonb"},
		{"course_departments", "folio_courses.coursereserves_departments", "jsonb"},
		{"course_processingstatuses", "folio_courses.coursereserves_processingstates", "jsonb"},
		{"course_reserves", "folio_courses.coursereserves_reserves", "jsonb"},
		{"course_roles", "folio_courses.coursereserves_roles", "jsonb"},
		{"course_terms", "folio_courses.coursereserves_terms", "jsonb"},
		{"email_email", "folio_email.email_statistics", "jsonb"},
		{"feesfines_accounts", "folio_feesfines.accounts", "jsonb"},
		{"feesfines_comments", "folio_feesfines.comments", "jsonb"},
		{"feesfines_feefineactions", "folio_feesfines.feefineactions", "jsonb"},
		{"feesfines_feefines", "folio_feesfines.feefines", "jsonb"},
		{"feesfines_lost_item_fees_policies", "folio_feesfines.lost_item_fee_policy", "jsonb"},
		{"feesfines_manualblocks", "folio_feesfines.manualblocks", "jsonb"},
		{"feesfines_overdue_fines_policies", "folio_feesfines.overdue_fine_policy", "jsonb"},
		{"feesfines_owners", "folio_feesfines.owners", "jsonb"},
		{"feesfines_payments", "folio_feesfines.payments", "jsonb"},
		{"feesfines_refunds", "folio_feesfines.refunds", "jsonb"},
		{"feesfines_transfer_criterias", "folio_feesfines.transfer_criteria", "jsonb"},
		{"feesfines_transfers", "folio_feesfines.transfers", "jsonb"},
		{"feesfines_waives", "folio_feesfines.waives", "jsonb"},
		{"finance_budgets", "folio_finance.budget", "jsonb"},
		{"finance_expense_classes", "folio_finance.expense_class", "jsonb"},
		{"finance_fiscal_years", "folio_finance.fiscal_year", "jsonb"},
		{"finance_fund_types", "folio_finance.fund_type", "jsonb"},
		{"finance_funds", "folio_finance.fund", "jsonb"},
		{"finance_group_fund_fiscal_years", "folio_finance.group_fund_fiscal_year", "jsonb"},
		{"finance_groups", "folio_finance.groups", "jsonb"},
		{"finance_ledgers", "folio_finance.ledger", "jsonb"},
		{"finance_transactions", "folio_finance.transaction", "jsonb"},
		{"inventory_alternative_title_types", "folio_inventory.alternative_title_type", "jsonb"},
		{"inventory_bound_with_part", "folio_inventory.bound_with_part", "jsonb"},
		{"inventory_call_number_types", "folio_inventory.call_number_type", "jsonb"},
		{"inventory_campuses", "folio_inventory.loccampus", "jsonb"},
		{"inventory_classification_types", "folio_inventory.classification_type", "jsonb"},
		{"inventory_contributor_name_types", "folio_inventory.contributor_name_type", "jsonb"},
		{"inventory_contributor_types", "folio_inventory.contributor_type", "jsonb"},
		{"inventory_electronic_access_relationships", "folio_inventory.electronic_access_relationship", "jsonb"},
		{"inventory_holdings", "folio_inventory.holdings_record", "jsonb"},
		{"inventory_holdings_note_types", "folio_inventory.holdings_note_type", "jsonb"},
		{"inventory_holdings_sources", "folio_inventory.holdings_records_source", "jsonb"},
		{"inventory_holdings_types", "folio_inventory.holdings_type", "jsonb"},
		{"inventory_identifier_types", "folio_inventory.identifier_type", "jsonb"},
		{"inventory_ill_policies", "folio_inventory.ill_policy", "jsonb"},
		{"inventory_instance_formats", "folio_inventory.instance_format", "jsonb"},
		{"inventory_instance_note_types", "folio_inventory.instance_note_type", "jsonb"},
		{"inventory_instance_relationship_types", "folio_inventory.instance_relationship_type", "jsonb"},
		{"inventory_instance_relationships", "folio_inventory.instance_relationship", "jsonb"},
		{"inventory_instance_statuses", "folio_inventory.instance_status", "jsonb"},
		{"inventory_instance_types", "folio_inventory.instance_type", "jsonb"},
		{"inventory_instances", "folio_inventory.instance", "jsonb"},
		{"inventory_institutions", "folio_inventory.locinstitution", "jsonb"},
		{"inventory_item_damaged_statuses", "folio_inventory.item_damaged_status", "jsonb"},
		{"inventory_item_note_types", "folio_inventory.item_note_type", "jsonb"},
		{"inventory_items", "folio_inventory.item", "jsonb"},
		{"inventory_libraries", "folio_inventory.loclibrary", "jsonb"},
		{"inventory_loan_types", "folio_inventory.loan_type", "jsonb"},
		{"inventory_locations", "folio_inventory.location", "jsonb"},
		{"inventory_material_types", "folio_inventory.material_type", "jsonb"},
		{"inventory_modes_of_issuance", "folio_inventory.mode_of_issuance", "jsonb"},
		{"inventory_nature_of_content_terms", "folio_inventory.nature_of_content_term", "jsonb"},
		{"inventory_service_points", "folio_inventory.service_point", "jsonb"},
		{"inventory_service_points_users", "folio_inventory.service_point_user", "jsonb"},
		{"inventory_statistical_code_types", "folio_inventory.statistical_code_type", "jsonb"},
		{"inventory_statistical_codes", "folio_inventory.statistical_code", "jsonb"},
		{"invoice_invoices", "folio_invoice.invoices", "jsonb"},
		{"invoice_lines", "folio_invoice.invoice_lines", "jsonb"},
		{"invoice_voucher_lines", "folio_invoice.voucher_lines", "jsonb"},
		{"invoice_vouchers", "folio_invoice.vouchers", "jsonb"},
		{"notes", "folio_notes.note", "jsonb"},
		{"organization_addresses", "folio_organizations.addresses", "jsonb"},
		{"organization_categories", "folio_organizations.categories", "jsonb"},
		{"organization_contacts", "folio_organizations.contacts", "jsonb"},
		{"organization_emails", "folio_organizations.emails", "jsonb"},
		{"organization_interfaces", "folio_organizations.interfaces", "jsonb"},
		{"organization_organizations", "folio_organizations.organizations", "jsonb"},
		{"organization_phone_numbers", "folio_organizations.phone_numbers", "jsonb"},
		{"organization_urls", "folio_organizations.urls", "jsonb"},
		{"patron_blocks_user_summary", "folio_patron_blocks.user_summary", "jsonb"},
		{"perm_permissions", "folio_permissions.permissions", "jsonb"},
		{"perm_users", "folio_permissions.permissions_users", "jsonb"},
		{"po_alerts", "folio_orders.alert", "jsonb"},
		{"po_lines", "folio_orders.po_line", "jsonb"},
		{"po_order_invoice_relns", "folio_orders.order_invoice_relationship", "jsonb"},
		{"po_order_templates", "folio_orders.order_templates", "jsonb"},
		{"po_pieces", "folio_orders.pieces", "jsonb"},
		{"po_purchase_orders", "folio_orders.purchase_order", "jsonb"},
		{"po_reporting_codes", "folio_orders.reporting_code", "jsonb"},
		{"template_engine_template", "folio_template_engine.template", "jsonb"},
		{"user_addresstypes", "folio_users.addresstype", "jsonb"},
		{"user_departments", "folio_users.departments", "jsonb"},
		{"user_groups", "folio_users.groups", "jsonb"},
		{"user_proxiesfor", "folio_users.proxyfor", "jsonb"},
		{"user_users", "folio_users.users", "jsonb"},
	}
	slices.SortFunc(m, func(a, b ldpTableMap) int {
		return cmp.Compare(a.metadbTable, b.metadbTable)
	})
	return m
}
