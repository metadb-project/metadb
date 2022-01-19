package server

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/metadb-project/metadb/cmd/metadb/cache"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
)

func addTable(table *sqlx.Table, db sqlx.DB, track *cache.Track, users *cache.Users) error {
	// if tracked, then assume the table exists
	if track.Contains(table) {
		return nil
	}
	// create tables
	if err := createSchemaIfNotExists(table, db, users); err != nil {
		return err
	}
	if err := createCurrentTableIfNotExists(table, db, users); err != nil {
		return err
	}
	if err := createHistoryTableIfNotExists(table, db, users); err != nil {
		return err
	}
	// track new table
	if err := track.Add(table); err != nil {
		return err
	}
	return nil
}

func createSchemaIfNotExists(table *sqlx.Table, db sqlx.DB, users *cache.Users) error {
	_, err := db.Exec(nil, "CREATE SCHEMA IF NOT EXISTS "+db.IdentiferSQL(table.Schema)+"")
	if err != nil {
		return err
	}
	for _, u := range users.WithPerm(table) {
		_, err := db.Exec(nil, "GRANT USAGE ON SCHEMA "+db.IdentiferSQL(table.Schema)+" TO "+u+"")
		if err != nil {
			log.Warning("%s", err)
		}
	}
	return nil
}

func createCurrentTableIfNotExists(table *sqlx.Table, db sqlx.DB, users *cache.Users) error {
	_, err := db.Exec(nil, ""+
		"CREATE TABLE IF NOT EXISTS "+db.TableSQL(table)+" ("+
		"    __id bigint "+db.AutoIncrementSQL()+" PRIMARY KEY,"+
		"    __cf boolean NOT NULL DEFAULT TRUE,"+
		"    __start timestamp with time zone NOT NULL,"+
		"    __origin varchar(63) NOT NULL DEFAULT ''"+
		")")
	if err != nil {
		return err
	}
	// Add indexes on new columns.
	_, err = db.Exec(nil, "CREATE INDEX ON "+db.TableSQL(table)+" (__start)")
	if err != nil {
		return err
	}
	_, err = db.Exec(nil, "CREATE INDEX ON "+db.TableSQL(table)+" (__origin)")
	if err != nil {
		return err
	}
	// Grant permissions on new table.
	for _, u := range users.WithPerm(table) {
		_, err := db.Exec(nil, "GRANT SELECT ON "+db.TableSQL(table)+" TO "+u+"")
		if err != nil {
			return err
		}
	}
	return nil
}

func createHistoryTableIfNotExists(table *sqlx.Table, db sqlx.DB, users *cache.Users) error {
	_, err := db.Exec(nil, ""+
		"CREATE TABLE IF NOT EXISTS "+db.HistoryTableSQL(table)+" ("+
		"    __id bigint "+db.AutoIncrementSQL()+" PRIMARY KEY,"+
		"    __cf boolean NOT NULL DEFAULT TRUE,"+
		"    __start timestamp with time zone NOT NULL,"+
		"    __end timestamp with time zone NOT NULL,"+
		"    __current boolean NOT NULL,"+
		"    __origin varchar(63) NOT NULL DEFAULT ''"+
		")")
	if err != nil {
		return err
	}
	// Add indexes on new columns.
	_, err = db.Exec(nil, "CREATE INDEX ON "+db.HistoryTableSQL(table)+" (__start)")
	if err != nil {
		return err
	}
	_, err = db.Exec(nil, "CREATE INDEX ON "+db.HistoryTableSQL(table)+" (__end)")
	if err != nil {
		return err
	}
	_, err = db.Exec(nil, "CREATE INDEX ON "+db.HistoryTableSQL(table)+" (__current)")
	if err != nil {
		return err
	}
	_, err = db.Exec(nil, "CREATE INDEX ON "+db.HistoryTableSQL(table)+" (__origin)")
	if err != nil {
		return err
	}
	// Grant permissions on new table.
	for _, u := range users.WithPerm(table) {
		_, err := db.Exec(nil, "GRANT SELECT ON "+db.HistoryTableSQL(table)+" TO "+u+"")
		if err != nil {
			return err
		}
	}
	return nil
}

func addColumn(table *sqlx.Table, columnName string, newType command.DataType, newTypeSize int64, db sqlx.DB, schema *cache.Schema) error {
	// Alter table schema in database.
	dataTypeSQL := command.DataTypeToSQL(newType, newTypeSize)
	_, err := db.Exec(nil, "ALTER TABLE "+db.TableSQL(table)+" ADD COLUMN "+db.IdentiferSQL(columnName)+" "+dataTypeSQL)
	if err != nil {
		return err
	}
	_, err = db.Exec(nil, "ALTER TABLE "+db.HistoryTableSQL(table)+" ADD COLUMN "+db.IdentiferSQL(columnName)+" "+dataTypeSQL)
	if err != nil {
		return err
	}
	// Add index on new column.
	if newType != command.JSONType && newTypeSize <= maximumTypeSizeIndex {
		_, err = db.Exec(nil, ""+
			"CREATE INDEX "+db.IdentiferSQL(indexName(table.Table, columnName))+
			" ON "+db.TableSQL(table)+" ("+db.IdentiferSQL(columnName)+")")
		if err != nil {
			return err
		}
		_, err = db.Exec(nil, ""+
			"CREATE INDEX "+db.IdentiferSQL(indexName(db.HistoryTable(table).Table, columnName))+
			" ON "+db.HistoryTableSQL(table)+" ("+db.IdentiferSQL(columnName)+")")
		if err != nil {
			return err
		}
	} else {
		log.Trace("disabling index: value too large")
	}
	// Update schema.
	dataType, charMaxLen := command.DataTypeToSQLNew(newType, newTypeSize)
	schema.Update(&sqlx.Column{Schema: table.Schema, Table: table.Table, Column: columnName}, dataType, charMaxLen)
	return nil
}

func alterColumnVarcharSize(table *sqlx.Table, column string, datatype command.DataType, typesize int64, db sqlx.DB, schema *cache.Schema) error {
	var err error
	// Remove index if type size too large.
	if typesize > maximumTypeSizeIndex {
		log.Trace("disabling index: value too large")
		_, err = db.Exec(nil, "DROP INDEX IF EXISTS "+db.IdentiferSQL(table.Schema)+"."+db.IdentiferSQL(indexName(table.Table, column)))
		if err != nil {
			return err
		}
		_, err = db.Exec(nil, "DROP INDEX IF EXISTS "+db.IdentiferSQL(table.Schema)+"."+db.IdentiferSQL(indexName(db.HistoryTable(table).Table, column)))
		if err != nil {
			return err
		}
	}
	// Alter table.
	_, err = db.Exec(nil, "ALTER TABLE "+db.TableSQL(table)+" ALTER COLUMN \""+column+"\" TYPE "+command.DataTypeToSQL(datatype, typesize))
	if err != nil {
		return err
	}
	_, err = db.Exec(nil, "ALTER TABLE "+db.HistoryTableSQL(table)+" ALTER COLUMN \""+column+"\" TYPE "+command.DataTypeToSQL(datatype, typesize))
	if err != nil {
		return err
	}
	// Update schema.
	dataType, charMaxLen := command.DataTypeToSQLNew(datatype, typesize)
	schema.Update(&sqlx.Column{Schema: table.Schema, Table: table.Table, Column: column}, dataType, charMaxLen)
	return nil
}

func renameColumnOldType(table *sqlx.Table, column string, datatype command.DataType, typesize int64, db sqlx.DB, schema *cache.Schema) error {
	var err error
	// Find new name for old column.
	var newName string
	if newName, err = newNumberedColumnName(table, column, schema); err != nil {
		return err
	}
	// Current table: rename column.
	_, err = db.Exec(nil, "ALTER TABLE "+db.TableSQL(table)+" RENAME COLUMN \""+column+"\" TO \""+newName+"\"")
	if err != nil {
		return err
	}
	// Current table: rename index.
	index := db.IdentiferSQL(table.Schema) + "." + db.IdentiferSQL(indexName(table.Table, column))
	_, err = db.Exec(nil, "ALTER INDEX IF EXISTS "+index+" RENAME TO "+db.IdentiferSQL(indexName(table.Table, newName)))
	if err != nil {
		return err
	}
	// History table: rename column.
	_, err = db.Exec(nil, "ALTER TABLE "+db.HistoryTableSQL(table)+" RENAME COLUMN \""+column+"\" TO \""+newName+"\"")
	if err != nil {
		return err
	}
	// History table: rename index.
	index = db.IdentiferSQL(table.Schema) + "." + db.IdentiferSQL(indexName(db.HistoryTable(table).Table, column))
	_, err = db.Exec(nil, "ALTER INDEX IF EXISTS "+index+" RENAME TO "+db.IdentiferSQL(indexName(db.HistoryTable(table).Table, newName)))
	if err != nil {
		return err
	}
	// Update schema.
	schema.Delete(&sqlx.Column{Schema: table.Schema, Table: table.Table, Column: column})
	dataType, charMaxLen := command.DataTypeToSQLNew(datatype, typesize)
	schema.Update(&sqlx.Column{Schema: table.Schema, Table: table.Table, Column: newName}, dataType, charMaxLen)
	return nil
}

func newNumberedColumnName(table *sqlx.Table, column string, schema *cache.Schema) (string, error) {
	var columns []string = schema.TableColumns(table)
	maxn := 0
	regex := regexp.MustCompile(`^` + column + `__([0-9]+)$`)
	for _, c := range columns {
		n := 0
		var match []string = regex.FindStringSubmatch(c)
		if match != nil {
			var err error
			if n, err = strconv.Atoi(match[1]); err != nil {
				return "", fmt.Errorf("internal error: column number: strconf.Atoi(): %s", err)
			}
		}
		if n > maxn {
			maxn = n
		}
	}
	newName := fmt.Sprintf("%s__%d", column, maxn+1)
	return newName, nil
}

func selectTableSchema(table *sqlx.Table, schema *cache.Schema) (*sysdb.TableSchema, error) {
	var m map[string]cache.ColumnType = schema.TableSchema(table)
	var ts = new(sysdb.TableSchema)
	for k, v := range m {
		name := k
		dtype, dtypesize := command.MakeDataTypeNew(v.DataType, v.CharMaxLen)
		cs := sysdb.ColumnSchema{Name: name, DType: dtype, DTypeSize: dtypesize}
		ts.Column = append(ts.Column, cs)
	}
	return ts, nil
}

func indexName(table, column string) string {
	return table + "_" + column + "_idx"
}

const maximumTypeSizeIndex = 2500
