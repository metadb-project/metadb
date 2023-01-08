package cat

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
	part := make(map[string]map[string]bool)
	for rows.Next() {
		var currentTable, yearTable string
		err := rows.Scan(&currentTable, &yearTable)
		if err != nil {
			return fmt.Errorf("reading partition years: %v", err)
		}
		p := part[currentTable]
		if p == nil {
			p = make(map[string]bool)
			part[currentTable] = p
		}
		year := yearTable[len(yearTable)-4:]
		if _, err := strconv.Atoi(year); err != nil {
			return fmt.Errorf("invalid partition: %s", yearTable)
		}
		p[year] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading partition years: %v", err)
	}
	c.partYears = part
	return nil
}

func (c *Catalog) AddPartYearIfNotExists(schema, table string, year string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Check if exists.
	if c.partYearExists(schema, table, year) {
		return nil
	}
	// Add partition in database.
	yearInt, err := strconv.Atoi(year)
	if err != nil {
		return fmt.Errorf("invalid partition year: %s", year)
	}
	nextYear := strconv.Itoa(yearInt + 1)
	q := "CREATE TABLE " + schema + ".zzz___" + table + "___" + year +
		" PARTITION OF " + schema + ".zzz___" + table + "___" +
		" FOR VALUES FROM ('" + year + "-01-01') to ('" + nextYear + "-01-01')"
	if _, err := c.dc.Exec(context.TODO(), q); err != nil {
		return err
	}
	// Update the cache.
	schemaTable := schema + "." + table
	p := c.partYears[schemaTable]
	if p == nil {
		p = make(map[string]bool)
		c.partYears[schemaTable] = p
	}
	p[year] = true
	return nil
}

func (c *Catalog) partYearExists(schema, table string, year string) bool {
	p := c.partYears[schema+"."+table]
	if p == nil {
		return false
	}
	return p[year]
}
