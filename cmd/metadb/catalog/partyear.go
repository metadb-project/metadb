package catalog

import (
	"context"
	"fmt"
	"strconv"
)

func (c *Catalog) initPartYears() error {
	q := "SELECT t.schemaname||'.'||t.tablename currenttable," +
		"i.inhrelid::regclass yeartable " +
		"FROM " + catalogSchema + ".track t " +
		"JOIN pg_class c ON 'zzz___'||t.tablename||'___'=c.relname " +
		"JOIN pg_namespace n ON c.relnamespace=n.oid AND t.schemaname=n.nspname " +
		"JOIN pg_partitioned_table p ON c.oid=p.partrelid " +
		"JOIN pg_inherits i ON p.partrelid=i.inhparent"
	rows, err := c.dc.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting partition years: %v", err)
	}
	defer rows.Close()
	part := make(map[string]map[int]bool)
	for rows.Next() {
		var currentTable, yearTable string
		err := rows.Scan(&currentTable, &yearTable)
		if err != nil {
			return fmt.Errorf("reading partition years: %v", err)
		}
		p := part[currentTable]
		if p == nil {
			p = make(map[int]bool)
			part[currentTable] = p
		}
		year := yearTable[len(yearTable)-4:]
		yearInt, err := strconv.Atoi(year)
		if err != nil {
			return fmt.Errorf("invalid partition: %s", yearTable)
		}
		p[yearInt] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading partition years: %v", err)
	}
	c.partYears = part
	return nil
}

func (c *Catalog) AddPartYearIfNotExists(schema, table string, year int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// If partition already exists, do nothing.
	if c.partYearExists(schema, table, year) {
		return nil
	}
	// Add partition in database.
	yearStr := strconv.Itoa(year)
	nextYearStr := strconv.Itoa(year + 1)
	nctable := "\"" + schema + "\".\"zzz___" + table + "___\""
	nctableYear := "\"" + schema + "\".\"zzz___" + table + "___" + yearStr + "\""
	q := "CREATE TABLE " + nctableYear +
		" PARTITION OF " + nctable +
		" FOR VALUES FROM ('" + yearStr + "-01-01') TO ('" + nextYearStr + "-01-01')"
	if _, err := c.dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	// Update the cache.
	schemaTable := schema + "." + table
	p := c.partYears[schemaTable]
	if p == nil {
		p = make(map[int]bool)
		c.partYears[schemaTable] = p
	}
	p[year] = true
	return nil
}

func (c *Catalog) partYearExists(schema, table string, year int) bool {
	p := c.partYears[schema+"."+table]
	if p == nil {
		return false
	}
	return p[year]
}
