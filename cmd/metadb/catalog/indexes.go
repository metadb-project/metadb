package catalog

import (
	"context"
	"fmt"
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/dbx"
)

func (c *Catalog) initIndexes() error {
	q := `WITH
attr AS (
    SELECT ns.nspname, t.relname, a.attnum, a.attname, FALSE AS has_index
        FROM metadb.base_table AS m
            JOIN pg_class AS t ON m.table_name||'__' = t.relname
            JOIN pg_namespace AS ns ON m.schema_name = ns.nspname AND t.relnamespace = ns.oid
            JOIN pg_attribute AS a ON t.oid = a.attrelid
        WHERE t.relkind IN ('r', 'p') AND a.attnum > 0
),
ind AS (
    SELECT d.nspname, d.relname, d.indname, d.attname, d.amname
        FROM ( SELECT ns.nspname,
                      t.relname,
                      i.relname AS indname,
                      a.attname,
                      ( SELECT c.rownum
                            FROM ( SELECT k, row_number() OVER () AS rownum
                                       FROM unnest(x.indkey) WITH ORDINALITY AS a (k)
                                 ) AS c
                            WHERE k = attnum
                      ),
                      am.amname
                   FROM metadb.base_table AS m
                       JOIN pg_class AS t ON m.table_name||'__' = t.relname
                       JOIN pg_namespace AS ns ON m.schema_name = ns.nspname AND t.relnamespace = ns.oid
                       JOIN pg_index AS x ON t.oid = x.indrelid
                       JOIN pg_class AS i ON x.indexrelid = i.oid
                       JOIN pg_attribute AS a
                           ON t.oid = a.attrelid AND a.attnum = ANY (x.indkey)
                       JOIN pg_opclass AS oc ON x.indclass[0] = oc.oid
                       JOIN pg_am AS am ON oc.opcmethod = am.oid
                   WHERE t.relkind IN ('r', 'p')
                   ORDER BY nspname, relname, indname, rownum
             ) AS d
),
part AS (
    SELECT nspname,
           relname,
           indname,
           first_value(attname) OVER (PARTITION BY nspname, relname, indname) AS attname,
           amname
        FROM ind
),
distpart AS (
    SELECT DISTINCT nspname,
                    relname,
                    attname,
                    TRUE AS has_index,
                    amname
        FROM part
),
joined AS (
    SELECT a.nspname::varchar AS table_schema,
           a.relname::varchar AS table_name,
           a.attname::varchar AS column_name,
           a.attnum AS ordinal_position,
           a.has_index OR coalesce(dp.has_index, FALSE) AS has_index,
           coalesce(dp.amname, '')::varchar AS index_type
        FROM attr AS a
            LEFT JOIN distpart AS dp ON a.nspname = dp.nspname AND a.relname = dp.relname AND a.attname = dp.attname
)
SELECT table_schema, table_name, column_name
    FROM joined
    WHERE has_index AND column_name NOT IN ('__id', '__start', '__end', '__current', '__origin')`
	rows, err := c.dp.Query(context.TODO(), q)
	if err != nil {
		return fmt.Errorf("selecting indexes: %w", err)
	}
	defer rows.Close()
	indexes := make(map[dbx.Column]struct{})
	for rows.Next() {
		var schema, table, column string
		if err := rows.Scan(&schema, &table, &column); err != nil {
			return fmt.Errorf("reading indexes: %w", err)
		}
		indexes[dbx.Column{Schema: schema, Table: strings.TrimSuffix(table, "__"), Column: column}] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading indexes: %w", err)
	}
	c.indexes = indexes
	return nil
}

func (c *Catalog) AddIndex(column *dbx.Column) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.addIndex(column)
}

func (c *Catalog) addIndex(column *dbx.Column) error {
	// Create index.
	q := "CREATE INDEX ON \"" + column.Schema + "\".\"" + column.Table + "__\" (\"" + column.Column + "\")"
	if _, err := c.dp.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating index: %w", err)
	}
	c.indexes[*column] = struct{}{}
	return nil
}

func (c *Catalog) IndexExists(column *dbx.Column) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.indexExists(column)
}

func (c *Catalog) indexExists(column *dbx.Column) bool {
	_, ok := c.indexes[*column]
	return ok
}
