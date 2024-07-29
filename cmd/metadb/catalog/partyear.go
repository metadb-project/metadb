package catalog

import (
	"context"
	"fmt"
	"strconv"
)

func (c *Catalog) initPartYears() error {
	q := "SELECT t.schema_name||'.'||t.table_name currenttable," +
		"i.inhrelid::regclass yeartable " +
		"FROM " + catalogSchema + ".base_table t " +
		"JOIN pg_class c ON 'zzz___'||t.table_name||'___'=c.relname " +
		"JOIN pg_namespace n ON c.relnamespace=n.oid AND t.schema_name=n.nspname " +
		"JOIN pg_partitioned_table p ON c.oid=p.partrelid " +
		"JOIN pg_inherits i ON p.partrelid=i.inhparent"
	rows, err := c.dp.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting partition years: %w", err)
	}
	defer rows.Close()
	part := make(map[string]map[int]struct{})
	for rows.Next() {
		var currentTable, yearTable string
		err := rows.Scan(&currentTable, &yearTable)
		if err != nil {
			return fmt.Errorf("reading partition years: %w", err)
		}
		p, ok := part[currentTable]
		if !ok {
			p = make(map[int]struct{})
			part[currentTable] = p
		}
		year := yearTable[len(yearTable)-4:]
		yearInt, err := strconv.Atoi(year)
		if err != nil {
			return fmt.Errorf("invalid partition: %s", yearTable)
		}
		p[yearInt] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading partition years: %w", err)
	}
	c.partYears = part
	return nil
}

func (c *Catalog) AddPartYear(schema, table string, year int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Add partition in database.
	yearStr := strconv.Itoa(year)
	nextYearStr := strconv.Itoa(year + 1)
	nctable := "\"" + schema + "\".\"zzz___" + table + "___\""
	nctableYear := "\"" + schema + "\".\"zzz___" + table + "___" + yearStr + "\""
	q := "CREATE TABLE " + nctableYear +
		" PARTITION OF " + nctable +
		" FOR VALUES FROM ('" + yearStr + "-01-01') TO ('" + nextYearStr + "-01-01')"
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating partition: %w", err)
	}
	// Update the cache.
	schemaTable := schema + "." + table
	p := c.partYears[schemaTable]
	if p == nil {
		p = make(map[int]struct{})
		c.partYears[schemaTable] = p
	}
	p[year] = struct{}{}
	return nil
}

func (c *Catalog) PartYearExists(schema, table string, year int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.partYearExists(schema, table, year)
}

func (c *Catalog) partYearExists(schema, table string, year int) bool {
	p := c.partYears[schema+"."+table]
	if p == nil {
		return false
	}
	_, ok := p[year]
	return ok
}
