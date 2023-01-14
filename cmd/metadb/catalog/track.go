package catalog

import (
	"context"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/dbx"
)

type tableEntry struct {
	transformed bool
	parentTable dbx.Table
	children    map[dbx.Table]struct{}
}

func (c *Catalog) initTableDir() error {
	q := "SELECT schemaname, tablename, transformed, parentschema, parenttable FROM metadb.track"
	rows, err := c.dc.Query(context.TODO(), q)
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
			tableDir[e.parentTable].children[t] = struct{}{}
		}
	}
	c.tableDir = tableDir
	return nil
}

func (c *Catalog) TableExists(table dbx.Table) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.tableDir[table]
	return ok
}

func (c *Catalog) AddTableEntry(table dbx.Table, transformed bool, parentTable dbx.Table) error {
	c.updateTableEntryWithLock(table, transformed, parentTable)
	q := "INSERT INTO " + catalogSchema + ".track(schemaname,tablename,transformed,parentschema,parenttable)VALUES($1,$2,$3,$4,$5)"
	if _, err := c.dc.Exec(context.TODO(), q, table.S, table.T, transformed, parentTable.S, parentTable.T); err != nil {
		return fmt.Errorf("writing table metadata: %s: %s", table, err)
	}
	return nil
}

func (c *Catalog) updateTableEntryWithLock(table dbx.Table, transformed bool, parentTable dbx.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	return nil
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
