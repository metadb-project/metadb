package catalog

import (
	"context"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
)

type tableEntry struct {
	transformed bool
	parentTable dbx.Table
	children    map[dbx.Table]struct{}
	source      string
}

func (c *Catalog) initTableDir() error {
	q := "SELECT schema_name, table_name, source_name, transformed, parent_schema_name, parent_table_name FROM metadb.base_table"
	rows, err := c.dp.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting table list: %w", err)
	}
	defer rows.Close()
	tableDir := make(map[dbx.Table]tableEntry)
	for rows.Next() {
		var schemaname, tablename, source, parentschema, parenttable string
		var transformed bool
		err = rows.Scan(&schemaname, &tablename, &source, &transformed, &parentschema, &parenttable)
		if err != nil {
			return fmt.Errorf("reading table list: %w", err)
		}
		t := tableEntry{
			transformed: transformed,
			parentTable: dbx.Table{Schema: parentschema, Table: parenttable},
			children:    make(map[dbx.Table]struct{}),
			source:      source,
		}
		tableDir[dbx.Table{Schema: schemaname, Table: tablename}] = t
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("reading table list: %w", err)
	}
	rows.Close()
	// Fill in children.
	for t, e := range tableDir {
		if e.transformed {
			if _, ok := tableDir[e.parentTable]; !ok {
				return fmt.Errorf("table %s has parent %s not found in catalog", t, e.parentTable)
			}
			tableDir[e.parentTable].children[t] = struct{}{}
		}
	}
	c.tableDir = tableDir
	return nil
}

func (c *Catalog) TableExists(table *dbx.Table) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return tableExists(c, table)
}

func tableExists(c *Catalog, table *dbx.Table) bool {
	_, ok := c.tableDir[*table]
	return ok
}

//func (c *Catalog) AddTableEntry(table dbx.Table, transformed bool, parentTable dbx.Table) error {
//	return addTableEntry(c, true, table, transformed, parentTable)
//}

func addTableEntry(c *Catalog, table *dbx.Table, transformed bool, parentTable *dbx.Table, source string) error {
	c.updateCacheTableEntry(table, transformed, parentTable)
	if err := insertIntoTableTrack(c, table, transformed, parentTable, source); err != nil {
		return fmt.Errorf("updating catalog in database for table %q: %v", table, err)
	}
	return nil
}

func insertIntoTableTrack(c *Catalog, table *dbx.Table, transformed bool, parentTable *dbx.Table, source string) error {
	q := "INSERT INTO " + catalogSchema +
		".base_table(schema_name,table_name,source_name,transformed,parent_schema_name,parent_table_name)VALUES($1,$2,$3,$4,$5,$6)"
	_, err := c.dp.Exec(context.TODO(), q, table.Schema, table.Table, source, transformed, parentTable.Schema, parentTable.Table)
	if err != nil {
		return fmt.Errorf("inserting catalog entry for table: %q: %s", table, err)
	}
	return nil
}

func (c *Catalog) updateCacheTableEntry(table *dbx.Table, transformed bool, parentTable *dbx.Table) {
	// If table exists, retain its children map.
	var children map[dbx.Table]struct{}
	t, ok := c.tableDir[*table]
	if ok {
		children = t.children
	} else {
		children = make(map[dbx.Table]struct{})
	}
	c.tableDir[*table] = tableEntry{
		transformed: transformed,
		parentTable: *parentTable,
		children:    children,
	}
	if parentTable.Schema != "" && parentTable.Table != "" {
		// In case the parent table entry has not yet been created, we create a stub where we can store
		// the children map.
		_, ok := c.tableDir[*parentTable]
		if !ok {
			c.tableDir[*parentTable] = tableEntry{children: make(map[dbx.Table]struct{})}
		}
		c.tableDir[*parentTable].children[*table] = struct{}{}
	}
}

func (c *Catalog) AllTables(source string) []dbx.Table {
	c.mu.Lock()
	defer c.mu.Unlock()
	all := make([]dbx.Table, 0)
	for t, e := range c.tableDir {
		if e.source != source {
			continue
		}
		all = append(all, t)
	}
	return all
}

func (c *Catalog) TraverseDescendantTables(table dbx.Table, process func(table dbx.Table)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traverseDescendantTables(table, process)
}

func (c *Catalog) traverseDescendantTables(table dbx.Table, process func(table dbx.Table)) {
	e, ok := c.tableDir[table]
	if !ok {
		return
	}
	process(table)
	for t := range e.children {
		c.traverseDescendantTables(t, process)
	}
}

/*func (c *Catalog) DescendantTables(table dbx.Table) []dbx.Table {
	c.mu.Lock()
	defer c.mu.Unlock()
	desc := make([]dbx.Table, 0)
	findDescendantTables(c.tableDir, table, &desc)
	return desc
}

func findDescendantTables(tableDir map[dbx.Table]tableEntry, table dbx.Table, desc *[]dbx.Table) {
	e, ok := tableDir[table]
	if !ok {
		return
	}
	*desc = append(*desc, table)
	for t := range e.children {
		findDescendantTables(tableDir, t, desc)
	}
}
*/

func (c *Catalog) CreateNewTable(table *dbx.Table, transformed bool, parentTable *dbx.Table, source string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := createSchemaIfNotExists(c, table); err != nil {
		return fmt.Errorf("creating new table %q: %v", table, err)
	}
	if err := createMainTableIfNotExists(c, table); err != nil {
		return fmt.Errorf("creating new table %q: %v", table, err)
	}
	if err := addTableEntry(c, table, transformed, parentTable, source); err != nil {
		return fmt.Errorf("creating new table %q: %v", table, err)
	}
	return nil
}

func createSchemaIfNotExists(c *Catalog, table *dbx.Table) error {
	q := "CREATE SCHEMA IF NOT EXISTS \"" + table.Schema + "\""
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating schema %q: %v", table.Schema, err)
	}
	for _, u := range usersWithPerm(c, table) {
		q = "GRANT USAGE ON SCHEMA \"" + table.Schema + "\" TO " + u
		if _, err := c.dp.Exec(context.TODO(), q); err != nil {
			log.Warning("granting privileges on schema %q to %q: %v", table.Schema, u, err)
		}
	}
	return nil
}

func IsReservedColumn(column string) bool {
	switch column {
	case "__id":
		return true
	case "__start":
		return true
	case "__end":
		return true
	case "__current":
		return true
	case "__origin":
		return true
	default:
		return false
	}
}

func createMainTableIfNotExists(c *Catalog, table *dbx.Table) error {
	q := "CREATE TABLE IF NOT EXISTS " + table.MainSQL() + " (" +
		"__id bigint GENERATED BY DEFAULT AS IDENTITY, " +
		"__start timestamp with time zone NOT NULL, " +
		"__end timestamp with time zone NOT NULL, " +
		"__current boolean NOT NULL, " +
		"__origin varchar(63) NOT NULL DEFAULT ''" +
		") PARTITION BY LIST (__current)"
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating partitioned table %q: %v", table.Main(), err)
	}
	q = "CREATE TABLE IF NOT EXISTS " + table.SQL() + " PARTITION OF " + table.MainSQL() + " FOR VALUES IN (TRUE)"
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating partition %q: %v", table, err)
	}
	partition := "zzz___" + table.Table + "___"
	nctable := "\"" + table.Schema + "\".\"" + partition + "\""
	q = "CREATE TABLE IF NOT EXISTS " + nctable + " PARTITION OF " + table.MainSQL() + " FOR VALUES IN (FALSE) " +
		"PARTITION BY RANGE (__start)"
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating partition %q: %v", table.Schema+"."+partition, err)
	}
	// Grant permissions on new tables.
	for _, u := range usersWithPerm(c, table) {
		if _, err := c.dp.Exec(context.TODO(), "GRANT SELECT ON "+table.MainSQL()+" TO "+u+""); err != nil {
			return fmt.Errorf("granting select privilege on %q to %q: %v", table.Main(), u, err)
		}
		if _, err := c.dp.Exec(context.TODO(), "GRANT SELECT ON "+table.SQL()+" TO "+u+""); err != nil {
			return fmt.Errorf("granting select privilege on %q to %q: %v", table, u, err)
		}
	}
	q = "CREATE INDEX ON " + table.MainSQL() + " (__id)"
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating index on table %q column \"__id\": %v", table.Main(), err)
	}
	// Create sync table.
	synctsql := SyncTable(table).SQL()
	q = "CREATE TABLE " + synctsql + " (__id bigint)"
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating sync table for %q: %v", table, err)
	}
	return nil
}

func SyncTable(table *dbx.Table) *dbx.Table {
	return &dbx.Table{
		Schema: table.Schema,
		Table:  "zzz___" + table.Table + "___sync",
	}
}
