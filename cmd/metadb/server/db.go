package server

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/metadb/cache"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func addTable(table *sqlx.Table, db *sqlx.DB, track *cache.Track, database *sysdb.DatabaseConnector) error {
	// if tracked, then assume the table exists
	if track.Contains(table) {
		return nil
	}
	// get db users
	users := []string{}
	u, err, _ := sysdb.GetConfig("db." + database.Name + ".users")
	if err != nil {
		return fmt.Errorf("reading database users: %s", err)
	}
	if strings.TrimSpace(u) != "" {
		users = util.SplitList(u)
	}
	// create tables
	if err = createSchemaIfNotExists(table.Schema, db, users); err != nil {
		return err
	}
	if err = createCurrentTableIfNotExists(table, db, users); err != nil {
		return err
	}
	if err = createHistoryTableIfNotExists(table, db, users); err != nil {
		return err
	}
	// track new table
	if err = track.Add(table); err != nil {
		return err
	}
	return nil
}

// database_general_user
func createSchemaIfNotExists(schema string, db *sqlx.DB, users []string) error {
	q := "CREATE SCHEMA IF NOT EXISTS \"" + schema + "\";"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	if len(users) > 0 {
		q = fmt.Sprintf("GRANT USAGE ON SCHEMA \"%s\" TO \"%s\";", schema, users[0])
		if _, err := db.ExecContext(context.TODO(), q); err != nil {
			log.Warning("granting permissions for users: %s", err)
		}
	}
	return nil
}

func createCurrentTableIfNotExists(table *sqlx.Table, db *sqlx.DB, users []string) error {
	q := "" +
		"CREATE TABLE IF NOT EXISTS " + table.SQL() + " (\n" +
		"    __id bigserial PRIMARY KEY,\n" +
		"    __start timestamp with time zone NOT NULL,\n" +
		"    __origin varchar(63) NOT NULL\n" +
		");"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating current table: %s", err)
	}
	// add indexes on new columns
	q = "CREATE INDEX ON " + table.SQL() + " (__start);"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		log.Error("unable to create index on " + table.SQL() + " (__start);")
	}
	q = "CREATE INDEX ON " + table.SQL() + " (__origin);"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		log.Error("unable to create index on " + table.SQL() + " (__origin);")
	}
	// grant permissions on new table
	if len(users) > 0 {
		q = "GRANT SELECT ON " + table.SQL() + " TO \"" + users[0] + "\";"
		if _, err := db.ExecContext(context.TODO(), q); err != nil {
			log.Warning("granting permissions for users: %s", err)
		}
	}
	return nil
}

func createHistoryTableIfNotExists(table *sqlx.Table, db *sqlx.DB, users []string) error {
	historyTableSQL := table.History().SQL()
	q := "" +
		"CREATE TABLE IF NOT EXISTS " + historyTableSQL + " (\n" +
		"    __id bigserial PRIMARY KEY,\n" +
		"    __current boolean NOT NULL,\n" +
		"    __start timestamp with time zone NOT NULL,\n" +
		"    __end timestamp with time zone NOT NULL,\n" +
		"    __origin varchar(63) NOT NULL\n" +
		");"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating history table: %s", err)
	}
	// add indexes on new columns
	q = "CREATE INDEX ON " + historyTableSQL + " (__current);"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		log.Error("unable to create index on " + historyTableSQL + " (__current);")
	}
	q = "CREATE INDEX ON " + historyTableSQL + " (__start);"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		log.Error("unable to create index on " + historyTableSQL + " (__start);")
	}
	q = "CREATE INDEX ON " + historyTableSQL + " (__end);"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		log.Error("unable to create index on " + historyTableSQL + " (__end);")
	}
	q = "CREATE INDEX ON " + historyTableSQL + " (__origin);"
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		log.Error("unable to create index on " + historyTableSQL + " (__origin);")
	}
	// grant permissions on new table
	if len(users) > 0 {
		q = fmt.Sprintf("GRANT SELECT ON " + historyTableSQL + " TO \"" + users[0] + "\";")
		if _, err := db.ExecContext(context.TODO(), q); err != nil {
			log.Warning("granting permissions for users: %s", err)
		}
	}
	return nil
}

func addColumn(table *sqlx.Table, columnName string, newType command.DataType, newTypeSize int64, db *sql.DB, schema *cache.Schema) error {
	// alter table schema in database
	q := fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"%s\" %s;", table.SQL(), columnName, command.DataTypeToSQL(newType, newTypeSize))
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("adding column %q: %s", columnName, err)
	}
	q = fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"%s\" %s;", table.History().SQL(), columnName, command.DataTypeToSQL(newType, newTypeSize))
	if _, err := db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("adding column %q: %s", columnName, err)
	}
	// add index on new column
	if newType != command.JSONType && newTypeSize <= maximumTypeSizeIndex {
		q = fmt.Sprintf("CREATE INDEX %s ON %s (\"%s\");", indexName(table.Table, columnName), table.SQL(), columnName)
		if _, err := db.ExecContext(context.TODO(), q); err != nil {
			log.Warning("unable to create index on table %v (\"%s\");", table, columnName)
		}
		q = fmt.Sprintf("CREATE INDEX %s ON %s (\"%s\");", indexName(table.HistoryTable(), columnName), table.History().SQL(), columnName)
		if _, err := db.ExecContext(context.TODO(), q); err != nil {
			log.Warning("unable to create index on table %v (\"%s\");", table.History(), columnName)
		}
	} else {
		log.Trace("disabling index: value too large")
	}
	// update schema
	dataType, charMaxLen := command.DataTypeToSQLNew(newType, newTypeSize)
	schema.Update(&sqlx.Column{Schema: table.Schema, Table: table.Table, Column: columnName}, dataType, charMaxLen)
	return nil
}

func alterColumnVarcharSize(table *sqlx.Table, column string, datatype command.DataType, typesize int64, db *sql.DB, schema *cache.Schema) error {
	var err error
	// remove index if type size too large
	if typesize > maximumTypeSizeIndex {
		log.Trace("disabling index: value too large")
		var q = fmt.Sprintf("DROP INDEX IF EXISTS \"%s\".%s;", table.Schema, indexName(table.Table, column))
		if _, err = db.ExecContext(context.TODO(), q); err != nil {
			eout.Warning("unable to drop index \"%s\".%s;", table.Schema, indexName(table.Table, column))
		}
		q = fmt.Sprintf("DROP INDEX IF EXISTS \"%s\".%s;", table.Schema, indexName(table.HistoryTable(), column))
		if _, err = db.ExecContext(context.TODO(), q); err != nil {
			eout.Warning("unable to drop index \"%s\".%s;", table.Schema, indexName(table.HistoryTable(), column))
		}
	}
	// alter table
	var q = fmt.Sprintf(""+
		"ALTER TABLE %s\n"+
		"    ALTER COLUMN \"%s\" TYPE %s;", table.SQL(), column, command.DataTypeToSQL(datatype, typesize))
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	q = fmt.Sprintf(""+
		"ALTER TABLE %s\n"+
		"    ALTER COLUMN \"%s\" TYPE %s;", table.History().SQL(), column, command.DataTypeToSQL(datatype, typesize))
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	// update schema
	dataType, charMaxLen := command.DataTypeToSQLNew(datatype, typesize)
	schema.Update(&sqlx.Column{Schema: table.Schema, Table: table.Table, Column: column}, dataType, charMaxLen)
	return nil
}

func renameColumnOldType(table *sqlx.Table, column string, datatype command.DataType, typesize int64, db *sql.DB, schema *cache.Schema) error {
	var err error
	// Find new name for old column
	var newName string
	if newName, err = newNumberedColumnName(table, column, schema); err != nil {
		return err
	}
	// Rename
	var q = fmt.Sprintf("ALTER TABLE %s RENAME COLUMN \"%s\" TO \"%s\";", table.SQL(), column, newName)
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	q = fmt.Sprintf("ALTER TABLE %s RENAME COLUMN \"%s\" TO \"%s\";", table.History().SQL(), column, newName)
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	// update schema
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
	var m map[string]cache.ColumnSchema = schema.TableSchema(table)
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

const maximumTypeSizeIndex = 4000
