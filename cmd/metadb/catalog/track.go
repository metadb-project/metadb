package catalog

import (
	"context"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

type tableEntry struct {
	transformed bool
	parentTable dbx.Table
	children    map[dbx.Table]struct{}
}

func (c *Catalog) initTableDir() error {
	q := "SELECT schemaname, tablename, transformed, parentschema, parenttable FROM metadb.track"
	rows, err := c.dp.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting table list: %v", err)
	}
	tableDir := make(map[dbx.Table]tableEntry)
	for rows.Next() {
		var schemaname, tablename, parentschema, parenttable string
		var transformed bool
		err = rows.Scan(&schemaname, &tablename, &transformed, &parentschema, &parenttable)
		if err != nil {
			rows.Close()
			return fmt.Errorf("reading table list: %v", err)
		}
		t := tableEntry{
			transformed: transformed,
			parentTable: dbx.Table{S: parentschema, T: parenttable},
			children:    make(map[dbx.Table]struct{}),
		}
		tableDir[dbx.Table{S: schemaname, T: tablename}] = t
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("reading table list: %v", err)
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

func (c *Catalog) TableExists(table dbx.Table) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return tableExists(c, table)
}

func tableExists(c *Catalog, table dbx.Table) bool {
	_, ok := c.tableDir[table]
	return ok
}

//func (c *Catalog) AddTableEntry(table dbx.Table, transformed bool, parentTable dbx.Table) error {
//	return addTableEntry(c, true, table, transformed, parentTable)
//}

func addTableEntry(c *Catalog, lock bool, table dbx.Table, transformed bool, parentTable dbx.Table) error {
	func(table dbx.Table, transformed bool, parentTable dbx.Table) {
		if lock {
			c.mu.Lock()
			defer c.mu.Unlock()
		}
		c.updateCacheTableEntry(table, transformed, parentTable)
	}(table, transformed, parentTable)
	if err := insertIntoTableTrack(c, table, transformed, parentTable); err != nil {
		return fmt.Errorf("updating catalog in database for table %q: %v", table, err)
	}
	return nil
}

func insertIntoTableTrack(c *Catalog, table dbx.Table, transformed bool, parentTable dbx.Table) error {
	q := "INSERT INTO " + catalogSchema + ".track(schemaname,tablename,transformed,parentschema,parenttable)VALUES($1,$2,$3,$4,$5)"
	if _, err := c.dp.Exec(context.TODO(), q, table.S, table.T, transformed, parentTable.S, parentTable.T); err != nil {
		return fmt.Errorf("inserting catalog entry for table: %q: %s", table, err)
	}
	return nil
}

func (c *Catalog) updateCacheTableEntry(table dbx.Table, transformed bool, parentTable dbx.Table) {
	// If table exists, retain its children map.
	var children map[dbx.Table]struct{}
	t, ok := c.tableDir[table]
	if ok {
		children = t.children
	} else {
		children = make(map[dbx.Table]struct{})
	}
	c.tableDir[table] = tableEntry{
		transformed: transformed,
		parentTable: parentTable,
		children:    children,
	}
	if parentTable.S != "" && parentTable.T != "" {
		// In case the parent table entry has not yet been created, we create a stub where we can store
		// the children map.
		_, ok := c.tableDir[parentTable]
		if !ok {
			c.tableDir[parentTable] = tableEntry{children: make(map[dbx.Table]struct{})}
		}
		c.tableDir[parentTable].children[table] = struct{}{}
	}
}

func (c *Catalog) AllTables() []dbx.Table {
	c.mu.Lock()
	defer c.mu.Unlock()
	all := make([]dbx.Table, 0)
	for t := range c.tableDir {
		all = append(all, t)
	}
	return all
}

func (c *Catalog) DescendantTables(table dbx.Table) []dbx.Table {
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

func (c *Catalog) CreateNewTable(table dbx.Table, transformed bool, parentTable dbx.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if tableExists(c, table) {
		return nil
	}
	if err := createSchemaIfNotExists(c, table); err != nil {
		return fmt.Errorf("creating new table %q: %v", table, err)
	}
	if err := createMainTableIfNotExists(c, table); err != nil {
		return fmt.Errorf("creating new table %q: %v", table, err)
	}
	if err := addTableEntry(c, false, table, transformed, parentTable); err != nil {
		return fmt.Errorf("creating new table %q: %v", table, err)
	}
	return nil
}

func createSchemaIfNotExists(c *Catalog, table dbx.Table) error {
	q := "CREATE SCHEMA IF NOT EXISTS \"" + table.S + "\""
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating schema %q: %v", table.S, err)
	}
	for _, u := range usersWithPerm(c, sqlx.NewTable(table.S, table.T)) {
		q = "GRANT USAGE ON SCHEMA \"" + table.S + "\" TO " + u
		if _, err := c.dp.Exec(context.TODO(), q); err != nil {
			log.Warning("granting privileges on schema %q to %q: %v", table.S, u, err)
		}
	}
	return nil
}

func createMainTableIfNotExists(c *Catalog, table dbx.Table) error {
	q := "CREATE TABLE IF NOT EXISTS " + table.MainSQL() + " (" +
		"__id bigint GENERATED BY DEFAULT AS IDENTITY, " +
		"__cf boolean NOT NULL DEFAULT TRUE, " +
		"__start timestamp with time zone NOT NULL, " +
		"__end timestamp with time zone NOT NULL, " +
		"__current boolean NOT NULL, " +
		"__source varchar(63) NOT NULL, " +
		"__origin varchar(63) NOT NULL DEFAULT ''" +
		") PARTITION BY LIST (__current)"
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating partitioned table %q: %v", table.Main(), err)
	}
	q = "CREATE TABLE IF NOT EXISTS " + table.SQL() + " PARTITION OF " + table.MainSQL() + " FOR VALUES IN (TRUE)"
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating partition %q: %v", table, err)
	}
	partition := "zzz___" + table.T + "___"
	nctable := "\"" + table.S + "\".\"" + partition + "\""
	q = "CREATE TABLE IF NOT EXISTS " + nctable + " PARTITION OF " + table.MainSQL() + " FOR VALUES IN (FALSE) " +
		"PARTITION BY RANGE (__start)"
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating partition %q: %v", table.S+"."+partition, err)
	}
	// Grant permissions on new tables.
	for _, u := range usersWithPerm(c, sqlx.NewTable(table.S, table.T)) {
		if _, err := c.dp.Exec(context.TODO(), "GRANT SELECT ON "+table.MainSQL()+" TO "+u+""); err != nil {
			return fmt.Errorf("granting select privilege on %q to %q: %v", table.Main(), u, err)
		}
		if _, err := c.dp.Exec(context.TODO(), "GRANT SELECT ON "+table.SQL()+" TO "+u+""); err != nil {
			return fmt.Errorf("granting select privilege on %q to %q: %v", table, u, err)
		}
	}
	return nil
}
