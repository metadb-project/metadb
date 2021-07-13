package sysdb

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/option"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

type ColumnSchema struct {
	Name           string
	DType          command.DataType
	DTypeSize      int64
	DataSampleNull bool
	PrimaryKey     int
}

type TableSchema struct {
	Column []ColumnSchema
}

const maximumTypeSizeIndex = 4000

func AddTable(schema string, table string, maindb *sql.DB) error {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	var q = fmt.Sprintf(""+
		"SELECT rel_name\n"+
		"    FROM relation\n"+
		"    WHERE rel_schema = '%s' AND rel_name = '%s';", schema, table)
	var relName string
	err = db.QueryRowContext(context.TODO(), q).Scan(&relName)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	if err = createSchemaIfNotExists(schema, maindb); err != nil {
		return err
	}
	if err = createTableIfNotExists(schema, table, maindb); err != nil {
		return err
	}
	if err = createTableIfNotExists(schema, table+"__", maindb); err != nil {
		return err
	}
	q = fmt.Sprintf(""+
		"INSERT INTO relation\n"+
		"        (rel_schema, rel_name)\n"+
		"    VALUES\n"+
		"        ('%s', '%s');", schema, table)
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	return nil
}

// database_general_user
func createSchemaIfNotExists(schema string, maindb *sql.DB) error {
	var err error
	var q = fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS \"%s\";", schema)
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	q = fmt.Sprintf("GRANT USAGE ON SCHEMA \"%s\" TO \"%s\";", schema, option.GeneralUser)
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	return nil
}

func createTableIfNotExists(schema string, table string, maindb *sql.DB) error {
	var err error
	var schemaTable = util.JoinSchemaTable(schema, table)
	var q = fmt.Sprintf(""+
		"CREATE TABLE IF NOT EXISTS %s (\n"+
		"    __id BIGSERIAL PRIMARY KEY,\n"+
		"    __current BOOLEAN,\n"+
		"    __start timestamp with time zone,\n"+
		"    __end timestamp with time zone\n"+
		");", schemaTable)
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	// Add indexes on new columns.
	q = fmt.Sprintf("CREATE INDEX ON %s (__current);", schemaTable)
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		log.Error("unable to create index on %s (__current);", schemaTable)
	}
	q = fmt.Sprintf("CREATE INDEX ON %s (__start);", schemaTable)
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		log.Error("unable to create index on %s (__start);", schemaTable)
	}
	q = fmt.Sprintf("CREATE INDEX ON %s (__end);", schemaTable)
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		log.Error("unable to create index on %s (__end);", schemaTable)
	}
	// Grant permissions on new table.
	q = fmt.Sprintf("GRANT SELECT ON %s TO \"%s\";", schemaTable, option.GeneralUser)
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	return nil
}

func AddColumn(tschema string, tableName string, columnName string, newType command.DataType, newTypeSize int64, maindb *sql.DB) error {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	var schemaTable = util.JoinSchemaTable(tschema, tableName)
	var schemaTableH = util.JoinSchemaTable(tschema, tableName+"__")
	// Alter table schema in database
	var q = fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"%s\" %s;",
		schemaTable, columnName, command.DataTypeToSQL(newType, newTypeSize))
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	q = fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"%s\" %s;",
		schemaTableH, columnName, command.DataTypeToSQL(newType, newTypeSize))
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	// Add index on new column.
	if newType != command.JSONType && newTypeSize <= maximumTypeSizeIndex {
		q = fmt.Sprintf("CREATE INDEX %s ON %s (\"%s\");", indexName(tableName, columnName), schemaTable, columnName)
		if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
			log.Warning("unable to create index on %s (\"%s\");", schemaTable, columnName)
		}
		q = fmt.Sprintf("CREATE INDEX %s ON %s (\"%s\");", indexName(tableName+"__", columnName), schemaTableH, columnName)
		if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
			log.Warning("unable to create index on %s (\"%s\");", schemaTableH, columnName)
		}
	} else {
		log.Trace("disabling index: value too large")
	}
	// Update catalog
	q = fmt.Sprintf(""+
		"INSERT INTO attribute\n"+
		"    (rel_schema, rel_name, attr_name, attr_type, attr_type_size, pkey)\n"+
		"    VALUES\n"+
		"    ('%s', '%s', '%s', '%s', %d, 0);",
		tschema, tableName, columnName, newType, newTypeSize)
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	return nil
}

func indexName(table, column string) string {
	return table + "_" + column + "_idx"
}

func AlterColumnVarcharSize(schema string, table string, column string, datatype command.DataType, typesize int64, maindb *sql.DB) error {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	// Remove index if type size too large.
	if typesize > maximumTypeSizeIndex {
		log.Trace("disabling index: value too large")
		var q = fmt.Sprintf("DROP INDEX IF EXISTS \"%s\".%s;", schema, indexName(table, column))
		if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
			eout.Warning("unable to drop index \"%s\".%s;", schema, indexName(table, column))
		}
		q = fmt.Sprintf("DROP INDEX IF EXISTS \"%s\".%s;", schema, indexName(table+"__", column))
		if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
			eout.Warning("unable to drop index \"%s\".%s;", schema, indexName(table+"__", column))
		}
	}
	// Alter table
	var q = fmt.Sprintf(""+
		"ALTER TABLE %s\n"+
		"    ALTER COLUMN \"%s\" TYPE %s;", util.JoinSchemaTable(schema, table), column, command.DataTypeToSQL(datatype, typesize))
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	q = fmt.Sprintf(""+
		"ALTER TABLE %s\n"+
		"    ALTER COLUMN \"%s\" TYPE %s;", util.JoinSchemaTable(schema, table+"__"), column, command.DataTypeToSQL(datatype, typesize))
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	// Update catalog
	q = fmt.Sprintf(""+
		"UPDATE attribute\n"+
		"    SET attr_type_size = %d\n"+
		"    WHERE rel_schema = '%s' AND\n"+
		"          rel_name = '%s' AND\n"+
		"          attr_name = '%s';", typesize, schema, table, column)
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	return nil
}

func RenameColumnOldType(schema string, table string, column string, datatype command.DataType, typesize int64, maindb *sql.DB) error {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	// Find new name for old column
	var newName string
	if newName, err = newNumberedColumnName(schema, table, column); err != nil {
		return err
	}
	// Rename
	var q = fmt.Sprintf("ALTER TABLE %s RENAME COLUMN \"%s\" TO \"%s\";", util.JoinSchemaTable(schema, table), column, newName)
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	q = fmt.Sprintf("ALTER TABLE %s RENAME COLUMN \"%s\" TO \"%s\";", util.JoinSchemaTable(schema, table+"__"), column, newName)
	if _, err = maindb.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	q = fmt.Sprintf(""+
		"UPDATE attribute\n"+
		"    SET attr_name = '%s'\n"+
		"    WHERE rel_schema = '%s' AND\n"+
		"          rel_name = '%s' AND\n"+
		"          attr_name = '%s';", newName, schema, table, column)
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	return nil
}

func newNumberedColumnName(schema string, table string, column string) (string, error) {
	var err error
	var q = fmt.Sprintf(""+
		"SELECT attr_name\n"+
		"    FROM attribute\n"+
		"    WHERE rel_schema = '%s' and rel_name = '%s';",
		schema, table)
	var rows *sql.Rows
	if rows, err = db.QueryContext(context.TODO(), q); err != nil {
		return "", fmt.Errorf("%s:\n%s", err, q)
	}
	defer rows.Close()
	var maxn int
	var regex = regexp.MustCompile(`^` + column + `__([0-9]+)$`)
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return "", err
		}
		var n int
		var match []string = regex.FindStringSubmatch(name)
		if match != nil {
			if n, err = strconv.Atoi(match[1]); err != nil {
				return "", fmt.Errorf("internal error: column number: strconf.Atoi(): %s", err)
			}
		}
		if n > maxn {
			maxn = n
		}
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	var newName = fmt.Sprintf("%s__%d", column, maxn+1)
	return newName, nil
}

func SelectTableSchema(schema string, table string) (*TableSchema, error) {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	var q = fmt.Sprintf(""+
		"SELECT attr_name,\n"+
		"       attr_type,\n"+
		"       attr_type_size,\n"+
		"       pkey\n"+
		"    FROM attribute\n"+
		"    WHERE rel_schema = '%s' and rel_name = '%s';",
		schema, table)
	var rows *sql.Rows
	if rows, err = db.QueryContext(context.TODO(), q); err != nil {
		return nil, fmt.Errorf("%s:\n%s", err, q)
	}
	defer rows.Close()
	var ts = new(TableSchema)
	for rows.Next() {
		var cs ColumnSchema
		var dtype string
		if err = rows.Scan(&cs.Name, &dtype, &cs.DTypeSize, &cs.PrimaryKey); err != nil {
			return nil, err
		}
		cs.DType = command.MakeDataType(dtype)

		ts.Column = append(ts.Column, cs)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return ts, nil
}
