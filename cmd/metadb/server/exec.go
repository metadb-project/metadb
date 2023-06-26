package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
)

func execCommandList(cat *catalog.Catalog, cl *command.CommandList, db sqlx.DB, dp *pgxpool.Pool) error {
	var clt []command.CommandList = partitionTxn(cat, cl)
	for _, cc := range clt {
		if len(cc.Cmd) == 0 {
			continue
		}
		// exec schema changes
		if err := execCommandSchema(cat, &cc.Cmd[0], db); err != nil {
			return fmt.Errorf("exec command schema: %v", err)
		}
		if err := execCommandAddIndexes(cat, cc); err != nil {
			return fmt.Errorf("exec command indexes: %v", err)
		}
		if err := execCommandListData(cat, db, dp, cc); err != nil {
			return fmt.Errorf("exec command data: %v", err)
		}
		// log confirmation
		if log.IsLevelTrace() {
			for _, c := range cc.Cmd {
				logTraceCommand(&c)
			}
		}
	}
	return nil
}

func execCommandSchema(cat *catalog.Catalog, cmd *command.Command, db sqlx.DB) error {

	if cmd.Op == command.DeleteOp || cmd.Op == command.TruncateOp {
		return nil
	}
	var err error
	var delta *deltaSchema
	if delta, err = findDeltaSchema(cat, cmd); err != nil {
		return fmt.Errorf("schema: %v", err)
	}
	if err = addTable(cmd, db, cat); err != nil {
		return fmt.Errorf("schema: %v", err)
	}
	if err = addPartition(cat, cmd); err != nil {
		return fmt.Errorf("schema: %v", err)
	}
	// Note that execDeltaSchema() may adjust data types in cmd.
	if err = execDeltaSchema(cat, cmd, delta, cmd.SchemaName, cmd.TableName, db); err != nil {
		return fmt.Errorf("schema: %v", err)
	}
	return nil
}

func execDeltaSchema(cat *catalog.Catalog, cmd *command.Command, delta *deltaSchema, tschema string, tableName string, db sqlx.DB) error {
	//if len(delta.column) == 0 {
	//        log.Trace("table %s: no schema changes", util.JoinSchemaTable(tschema, tableName))
	//}
	for _, col := range delta.column {
		// Is this a new column (as opposed to a modification)?
		if col.newColumn {
			dtypesql := command.DataTypeToSQL(col.newType, col.newTypeSize)
			log.Trace("table %s.%s: new column: %s %s", tschema, tableName, col.name, dtypesql)
			t := dbx.Table{S: tschema, T: tableName}
			if err := cat.AddColumn(t, col.name, col.newType, col.newTypeSize); err != nil {
				return fmt.Errorf("delta schema: %v", err)
			}
			continue
		}
		// If the type is changing from varchar to another type, keep
		// the type as varchar and let the executor cast the data.
		// This is to prevent poorly typed JSON fields from causing
		// runaway type changes (and the resulting runaway column
		// renaming).  Later we can give the user a way to change the
		// type of a specific column.
		if col.oldType == command.TextType && col.newType != command.TextType {
			// Adjust the new data type in the command.
			var typeSize int64 = -1
			for j, c := range cmd.Column {
				if c.Name == col.name {
					if cmd.Column[j].SQLData == nil {
						typeSize = 0
					} else {
						typeSize = int64(len(*(cmd.Column[j].SQLData)))
					}
					cmd.Column[j].DType = command.TextType
					cmd.Column[j].DTypeSize = typeSize
					break
				}
			}
			if typeSize == -1 {
				return fmt.Errorf("delta schema: internal error: column not found in command: %s.%s (%s)", tschema, tableName, col.name)
			}
			if typeSize <= col.oldTypeSize {
				continue
			}
			// Change the delta column type as well so that column
			// size can be adjusted below if needed.
			col.newType = command.TextType
			col.newTypeSize = typeSize
		}

		// Don't change a UUID type with a null value, because UUID may have been inferred from data.
		if col.oldType == command.UUIDType && col.newType == command.TextType && col.newData == nil {
			continue
		}

		// If both the old and new types are varchar, most databases
		// can alter the column in place.
		/*		if col.oldType == command.TextType && col.newType == command.TextType {
					dtypesql, _, _ := command.DataTypeToSQL(col.newType, col.newTypeSize)
					log.Trace("table %s.%s: alter column: %s %s", tschema, tableName, col.name, dtypesql)
					if err := alterColumnVarcharSize(cat, sqlx.NewTable(tschema, tableName), col.name, col.newType, col.newTypeSize, db); err != nil {
						return fmt.Errorf("delta schema: %v", err)
					}
					continue
				}
		*/
		// If both the old and new types are IntegerType, change the
		// column type to handle the larger size.
		if col.oldType == command.IntegerType && col.newType == command.IntegerType {
			// err := alterColumnIntegerSize(sqlx.NewTable(tschema, tableName), col.name, col.newTypeSize, db, schema)
			err := alterColumnType(cat, db, tschema, tableName, col.name, command.IntegerType, col.newTypeSize, false)
			if err != nil {
				return fmt.Errorf("delta schema: %v", err)
			}
			continue
		}

		// If both the old and new types are FloatType, change the
		// column type to handle the larger size.
		if col.oldType == command.FloatType && col.newType == command.FloatType {
			// err := alterColumnFloatSize(sqlx.NewTable(tschema, tableName), col.name, col.newTypeSize, db, schema)
			err := alterColumnType(cat, db, tschema, tableName, col.name, command.FloatType, col.newTypeSize, false)
			if err != nil {
				return fmt.Errorf("delta schema: %v", err)
			}
			continue
		}

		// If this is a change from an integer to float type, the
		// column type can be changed using a cast.
		if col.oldType == command.IntegerType && col.newType == command.FloatType {
			err := alterColumnType(cat, db, tschema, tableName, col.name, command.FloatType, col.newTypeSize, false)
			if err != nil {
				return fmt.Errorf("delta schema: %v", err)
			}
			continue
		}

		// If this is a change from an integer or float to numeric
		// type, the column type can be changed using a cast.
		if (col.oldType == command.IntegerType || col.oldType == command.FloatType) && col.newType == command.NumericType {
			err := alterColumnType(cat, db, tschema, tableName, col.name, command.NumericType, 0, false)
			if err != nil {
				return fmt.Errorf("delta schema: %v", err)
			}
			continue
		}

		// If this is a change from a float to integer type, cast the
		// column to the numeric type.
		if col.oldType == command.FloatType && col.newType == command.IntegerType {
			err := alterColumnType(cat, db, tschema, tableName, col.name, command.NumericType, 0, false)
			if err != nil {
				return fmt.Errorf("delta schema: %v", err)
			}
			continue
		}

		// Prevent conversion from numeric to integer or float type.
		if col.oldType == command.NumericType && (col.newType == command.IntegerType || col.newType == command.FloatType) {
			continue
		}

		// If not a compatible change, adjust new type to varchar in all cases.

		/*
			// To do this we need to determine what the varchar length limit should be, by
			// looking at existing data and the new datum.

			// Get maximum string length of existing data.
			maxlen, err := selectMaxStringLength(db, sqlx.NewTable(tschema, tableName), col.name)
			if err != nil {
				return fmt.Errorf("delta schema: %v", err)
			}

			// Get string length of new datum.
			var typeSize int64 = -1
			for j, c := range cmd.Column {
				if c.Name == col.name {
					if cmd.Column[j].SQLData == nil {
						typeSize = 0
					} else {
						typeSize = int64(len(*(cmd.Column[j].SQLData)))
					}
					cmd.Column[j].DType = command.TextType
					cmd.Column[j].DTypeSize = typeSize
					break
				}
			}
			if typeSize == -1 {
				return fmt.Errorf("delta schema: internal error: column %q in table %q not found in command: ",
					col.name, tschema+"."+tableName)
			}

			if typeSize > maxlen {
				maxlen = typeSize
			}
			if maxlen < 1 {
				maxlen = 1
			}
		*/

		// err = alterColumnToVarchar(sqlx.NewTable(tschema, tableName), col.name, maxlen, db, schema)
		err := alterColumnType(cat, db, tschema, tableName, col.name, command.TextType, 0, false)
		if err != nil {
			return fmt.Errorf("delta schema: %v", err)
		}

		/* Old renaming method:
		log.Trace("table %s.%s: rename column %s", tschema, tableName, col.name)
		if err := renameColumnOldType(sqlx.NewTable(tschema, tableName), col.name, col.newType, col.newTypeSize, db, schema); err != nil {
			return err
		}
		dtypesql, _, _ := command.DataTypeToSQL(col.newType, col.newTypeSize)
		log.Trace("table %s.%s: new column %s %s", tschema, tableName, col.name, dtypesql)
		t := &sqlx.T{S: tschema, T: tableName}
		if err := addColumn(t, col.name, col.newType, col.newTypeSize, db, schema); err != nil {
			return err
		}
		*/
	}
	return nil
}

/*func selectMaxStringLength(db sqlx.DB, table *sqlx.Table, column string) (int64, error) {
	var maxlen int64
	tx, err := db.BeginTx()
	if err != nil {
		return 0, fmt.Errorf("computing maximum string length of column %q in table %q: begin transaction: %v",
			column, table, err)
	}
	defer tx.Rollback()
	q := "SELECT coalesce(max(length(\"" + column + "\"::varchar)), 0) FROM " + db.HistoryTableSQL(table)
	if err = db.QueryRow(tx, q).Scan(&maxlen); err != nil {
		return 0, fmt.Errorf("computing maximum string length of column %q in table %q: select: %v",
			column, table, err)
	}
	return maxlen, nil
}
*/

func execCommandAddIndexes(cat *catalog.Catalog, cmds command.CommandList) error {
	for _, cmd := range cmds.Cmd {
		// The table associated with delete/truncate operations may not exist, and any
		// needed indexes would have been created anyway with a merge operation.
		if cmd.Op == command.DeleteOp || cmd.Op == command.TruncateOp {
			continue
		}
		// Create indexes on primary key columns.
		for _, col := range cmd.Column {
			if col.PrimaryKey != 0 {
				if err := cat.AddIndexIfNotExists(cmd.SchemaName, cmd.TableName, col.Name); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func execCommandListData(cat *catalog.Catalog, db sqlx.DB, dp *pgxpool.Pool, cc command.CommandList) error {
	tx, err := dp.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	// Exec data
	for _, c := range cc.Cmd {
		// Execute data part of command
		if err = execCommandData(cat, &c, tx, db); err != nil {
			return fmt.Errorf("data: %v", err)
		}
	}
	// Commit txn
	log.Trace("commit txn")
	if err = tx.Commit(context.TODO()); err != nil {
		return fmt.Errorf("committing changes: %v", err)
	}
	return nil
}

func partitionTxn(cat *catalog.Catalog, cl *command.CommandList) []command.CommandList {
	var clt []command.CommandList
	newcl := new(command.CommandList)
	var c, lastc command.Command
	for _, c = range cl.Cmd {
		req := requiresSchemaChanges(cat, &c, &lastc)
		if req {
			if len(newcl.Cmd) > 0 {
				clt = append(clt, *newcl)
				newcl = new(command.CommandList)
			}
		}
		newcl.Cmd = append(newcl.Cmd, c)
		lastc = c
	}
	if len(newcl.Cmd) > 0 {
		clt = append(clt, *newcl)
	}
	return clt
}

func requiresSchemaChanges(cat *catalog.Catalog, c, o *command.Command) bool {
	if c.Op == command.DeleteOp || c.Op == command.TruncateOp {
		return false
	}
	if c.Op != o.Op || c.SchemaName != o.SchemaName || c.TableName != o.TableName {
		return true
	}
	if len(c.Column) != len(o.Column) {
		return true
	}
	for i, col := range c.Column {
		switch {
		case col.Name != o.Column[i].Name:
			return true
		case col.DType != o.Column[i].DType:
			return true
		case col.DTypeSize != o.Column[i].DTypeSize:
			return true
		case col.PrimaryKey != o.Column[i].PrimaryKey:
			return true
		}
	}
	return false
}

func execCommandData(cat *catalog.Catalog, c *command.Command, tx pgx.Tx, db sqlx.DB) error {
	switch c.Op {
	case command.MergeOp:
		if err := execMergeData(c, tx, db); err != nil {
			return fmt.Errorf("merge: %v", err)
		}
	case command.DeleteOp:
		if err := execDeleteData(cat, c, tx, db); err != nil {
			return fmt.Errorf("delete: %v", err)
		}
	case command.TruncateOp:
		if err := execTruncateData(cat, c, tx, db); err != nil {
			return fmt.Errorf("truncate: %v", err)
		}
	default:
		return fmt.Errorf("unknown command op: %v", c.Op)
	}
	return nil
}

// execMergeData executes a merge command in the database.
func execMergeData(cmd *command.Command, tx pgx.Tx, db sqlx.DB) error {
	table := dbx.Table{S: cmd.SchemaName, T: cmd.TableName}
	// Check if the current record (if any) is identical to the new one.  If so, we
	// can avoid making any changes in the database.
	ident, cf, err := isCurrentIdentical(cmd, tx, &table, db)
	if err != nil {
		return fmt.Errorf("matcher: %v", err)
	}
	if ident {
		////////////////////////////////////////////////////////////////////////////////
		// Temporary: read __cf value which, if false, currently still requires updating
		// the row.
		if cf == false {
			// log.Trace("matcher cf")
			return updateRowCF(cmd, tx, db, &table)
		}
		////////////////////////////////////////////////////////////////////////////////
		// log.Trace("matcher ok")
		return nil
	}
	// If any columns are "unavailable," extract the previous values from the current record.
	unavail := make([]string, 0)
	for _, c := range cmd.Column {
		if c.Unavailable {
			unavail = append(unavail, c.Name)
		}
	}
	if len(unavail) != 0 {
		var b strings.Builder
		b.WriteString("SELECT ")
		for i, n := range unavail {
			if i != 0 {
				b.WriteByte(',')
			}
			b.WriteByte('"')
			b.WriteString(n)
			b.WriteString("\"::text")
		}
		b.WriteString(" FROM \"")
		b.WriteString(table.S)
		b.WriteString("\".\"")
		b.WriteString(table.T)
		b.WriteString("\" WHERE __origin='")
		b.WriteString(cmd.Origin)
		b.WriteString("'")
		if err = wherePKDataEqual(db, &b, cmd.Column); err != nil {
			return fmt.Errorf("primary key columns equal: %v", err)
		}
		b.WriteString(" LIMIT 1")
		var rows pgx.Rows
		rows, err = tx.Query(context.TODO(), b.String())
		if err != nil {
			return fmt.Errorf("querying for unavailable data: %v", err)
		}
		defer rows.Close()
		lenColumns := len(unavail)
		dest := make([]any, lenColumns)
		values := make([]any, lenColumns)
		for i := range dest {
			dest[i] = &(values[i])
		}
		found := false
		if rows.Next() {
			found = true
			if err = rows.Scan(dest...); err != nil {
				return fmt.Errorf("scanning row values: %v", err)
			}
		}
		if err = rows.Err(); err != nil {
			return fmt.Errorf("reading matching current row: %v", err)
		}
		rows.Close()
		if !found {
			log.Warning("no current value for unavailable data in table %q", table)
		} else {
			for i, c := range cmd.Column {
				if c.Unavailable {
					for j := 0; j < lenColumns; j++ {
						if c.Name == unavail[j] {
							if values[j] == nil {
								return fmt.Errorf("nil value in replacing" +
									" unavailable data")
							}
							s, ok := values[j].(string)
							if !ok {
								return fmt.Errorf("type assertion failed in" +
									" replacing unavailable data")
							}
							cmd.Column[i].SQLData = &s
							log.Trace("found current value for unavailable data: %v",
								cmd.Column[i])
							break
						}
					}
				}
			}
		}
	}
	// Set the current row, if any, to __current=FALSE.
	var b strings.Builder
	b.WriteString("UPDATE \"")
	b.WriteString(table.S)
	b.WriteString("\".\"")
	b.WriteString(table.T)
	b.WriteString("__\" SET __cf='t',__end='")
	b.WriteString(cmd.SourceTimestamp)
	b.WriteString("',__current='f'")
	b.WriteString(" WHERE __current AND __origin='")
	b.WriteString(cmd.Origin)
	b.WriteByte('\'')
	if err = wherePKDataEqual(db, &b, cmd.Column); err != nil {
		return fmt.Errorf("primary key columns equal: %v", err)
	}
	if _, err := tx.Exec(context.TODO(), b.String()); err != nil {
		return fmt.Errorf("updating current row: %v", err)
	}
	// Insert the new row.
	b.Reset()
	b.WriteString("INSERT INTO \"")
	b.WriteString(table.S)
	b.WriteString("\".\"")
	b.WriteString(table.T)
	b.WriteString("__\"(__start,__end,__current")
	if cmd.Origin != "" {
		b.WriteString(",__origin")
	}
	for _, c := range cmd.Column {
		b.WriteString(",\"")
		b.WriteString(c.Name)
		b.WriteByte('"')
	}
	b.WriteString(")VALUES('")
	b.WriteString(cmd.SourceTimestamp)
	b.WriteString("','9999-12-31 00:00:00Z','t'")
	if cmd.Origin != "" {
		b.WriteString(",'")
		b.WriteString(cmd.Origin)
		b.WriteByte('\'')
	}
	for _, c := range cmd.Column {
		b.WriteString(",")
		b.WriteString(encodeSQLData(c.SQLData, c.DType, db))
	}
	b.WriteByte(')')
	if _, err := tx.Exec(context.TODO(), b.String()); err != nil {
		return fmt.Errorf("inserting new row: %v", err)
	}
	return nil
}

/*func oldExecMergeData(c *command.Command, tx pgx.Tx, db sqlx.DB) error {
	t := sqlx.Table{Schema: c.SchemaName, Table: c.TableName}
	// Check if current record is identical.
	ident, cf, err := isCurrentIdentical(c, tx, &t)
	if err != nil {
		return fmt.Errorf("matcher: %v", err)
	}
	if ident {
		if cf == false {
			// log.Trace("matcher cf")
			return updateRowCF(c, tx, db, &t)
		}
		// log.Trace("matcher ok")
		return nil
	}
	// History table:
	// Select matching current record in history table and mark as not current.
	var uphist strings.Builder
	uphist.WriteString("UPDATE " + db.HistoryTableSQL(&t) + " SET __cf=TRUE,__end='" + c.SourceTimestamp + "',__current=FALSE WHERE __current AND __origin='" + c.Origin + "'")
	if err = wherePKDataEqual(db, &uphist, c.Column); err != nil {
		return fmt.Errorf("primary key columns equal: %v", err)
	}
	if _, err := tx.Exec(context.TODO(), uphist.String()); err != nil {
		return fmt.Errorf("updating current row: %v", err)
	}
	// insert new record
	var inshist strings.Builder
	inshist.WriteString("INSERT INTO " + db.TableSQL(&t) + "(__start,__end,__current")
	if c.Origin != "" {
		inshist.WriteString(",__origin")
	}
	for _, c := range c.Column {
		inshist.WriteString("," + db.IdentiferSQL(c.Name))
	}
	inshist.WriteString(")VALUES('" + c.SourceTimestamp + "','9999-12-31 00:00:00Z',TRUE")
	if c.Origin != "" {
		inshist.WriteString(",'" + c.Origin + "'")
	}
	for _, c := range c.Column {
		inshist.WriteString("," + encodeSQLData(c.SQLData, c.DType, db))
	}
	inshist.WriteString(")")
	if _, err := tx.Exec(context.TODO(), inshist.String()); err != nil {
		return fmt.Errorf("inserting new row: %v", err)
	}
	return nil
}
*/

func updateRowCF(c *command.Command, tx pgx.Tx, db sqlx.DB, table *dbx.Table) error {
	// Select matching current record in history table.
	var uphist strings.Builder
	uphist.WriteString("UPDATE \"" + table.S + "\".\"" + table.T + "\" SET __cf=TRUE WHERE __origin='" + c.Origin +
		"'")
	if err := wherePKDataEqual(db, &uphist, c.Column); err != nil {
		return fmt.Errorf("primary key columns equal: %v", err)
	}
	if _, err := tx.Exec(context.TODO(), uphist.String()); err != nil {
		return fmt.Errorf("setting cf flag: %v", err)
	}
	return nil
}

// isCurrentIdentical looks for an identical row in the current table.
func isCurrentIdentical(cmd *command.Command, tx pgx.Tx, table *dbx.Table, db sqlx.DB) (bool, bool, error) {
	// Match on all columns, except "unavailable" columns (which indicates a column
	// did not change and we can assume it matches).
	var b strings.Builder
	b.WriteString("SELECT * FROM \"")
	b.WriteString(table.S)
	b.WriteString("\".\"")
	b.WriteString(table.T)
	b.WriteString("\" WHERE __origin='")
	b.WriteString(cmd.Origin)
	b.WriteByte('\'')
	for _, c := range cmd.Column {
		if c.Unavailable {
			continue
		}
		b.WriteString(" AND \"")
		b.WriteString(c.Name)
		if c.Data == nil {
			b.WriteString("\" IS NULL")
		} else {
			b.WriteString("\"=")
			b.WriteString(encodeSQLData(c.SQLData, c.DType, db))
		}
	}
	b.WriteString(" LIMIT 1")
	rows, err := tx.Query(context.TODO(), b.String())
	if err != nil {
		return false, false, fmt.Errorf("querying for matching current row: %v", err)
	}
	defer rows.Close()
	columnNames := make([]string, 0)
	for _, f := range rows.FieldDescriptions() {
		columnNames = append(columnNames, f.Name)
	}
	lenColumns := len(columnNames)
	found := false
	dest := make([]any, lenColumns)
	values := make([]any, lenColumns)
	for i := range dest {
		dest[i] = &(values[i])
	}
	if rows.Next() {
		found = true
		if err = rows.Scan(dest...); err != nil {
			return false, false, fmt.Errorf("scanning row values: %v", err)
		}
	}
	if err = rows.Err(); err != nil {
		return false, false, fmt.Errorf("reading matching current row: %v", err)
	}
	if !found {
		return false, false, nil
	}
	// If any extra column values are not NULL, there is no match.
	var cf bool
	for i := 0; i < lenColumns; i++ {
		//////////////////////////////////////////////////////////////////////////////////////
		// Temporary: read __cf value which, if false, currently still requires updating
		// the row.
		if columnNames[i] == "__cf" {
			var ok bool
			cf, ok = values[i].(bool)
			if !ok {
				return false, false, fmt.Errorf("error reading __cf as boolean value")
			}
		}
		//////////////////////////////////////////////////////////////////////////////////////
		found := false
		for _, c := range cmd.Column {
			if columnNames[i] == c.Name {
				found = true
				break
			}
		}
		if !found { // This is an extra column.
			if values[i] != nil {
				return false, false, nil
			}
		}
	}
	// Otherwise we have found a match.
	return true, cf, nil
}

func oldIsCurrentIdentical(c *command.Command, tx *sql.Tx, db sqlx.DB, t *sqlx.Table) (bool, string, error) {
	var b strings.Builder
	b.WriteString("SELECT * FROM " + db.TableSQL(t) + " WHERE __origin='" + c.Origin + "'")
	if err := wherePKDataEqual(db, &b, c.Column); err != nil {
		return false, "", fmt.Errorf("primary key columns equal: %v", err)
	}
	b.WriteString(" LIMIT 1")
	rows, err := db.Query(tx, b.String())
	if err != nil {
		return false, "", err
	}
	cols, err := rows.Columns()
	if err != nil {
		return false, "", fmt.Errorf("columns: %v", err)
	}
	ptrs := make([]interface{}, len(cols))
	results := make([][]byte, len(cols))
	for i := range results {
		ptrs[i] = &results[i]
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	var cf string
	attrs := make(map[string]*string)
	if rows.Next() {
		if err = rows.Scan(ptrs...); err != nil {
			return false, "", err
		}
		for i, r := range results {
			if r != nil {
				attr := cols[i]
				val := string(r)
				switch attr {
				case "__id":
				case "__cf":
					cf = val
				case "__start":
				case "__end":
				case "__current":
				case "__origin":
				default:
					v := new(string)
					*v = val
					attrs[attr] = v
				}
			}
		}
	} else {
		// log.Trace("matcher: %s: row not found in database: %s", t, command.ColumnsString(c.Column))
		return false, "", nil
	}
	for _, col := range c.Column {
		var cdata, ddata *string
		var cdatas, ddatas string
		cdata = col.SQLData
		if cdata != nil {
			cdatas = *cdata
		}
		ddata = attrs[col.Name]
		if ddata != nil {
			ddatas = *ddata
		}
		if (cdata == nil && ddata != nil) || (cdata != nil && ddata == nil) {
			// log.Trace("matcher: %s (%s): cdata(%v) != ddata(%v)", t, col.Name, cdata, ddata)
			return false, cf, nil
		}
		if cdata != nil && ddata != nil && cdatas != ddatas {
			// log.Trace("matcher: %s (%s): cdatas(%s) != ddatas(%s)", t, col.Name, cdatas, ddatas)
			return false, cf, nil
		}
		delete(attrs, col.Name)
	}
	// for k, v := range attrs {
	for _, v := range attrs {
		if v != nil {
			// log.Trace("matcher: %s (%s): database has extra value: %v", t, k, v)
			return false, cf, nil
		}
	}
	return true, cf, nil
}

func execDeleteData(cat *catalog.Catalog, c *command.Command, tx pgx.Tx, db sqlx.DB) error {
	// Get the transformed tables so that we can propagate the delete operation.
	tables := cat.DescendantTables(dbx.Table{S: c.SchemaName, T: c.TableName})
	// Note that if the table does not exist, "tables" will be an empty slice and the
	// loop below will not do anything.

	// Find matching current record and mark as not current.
	// TODO Use pgx.Batch
	for _, t := range tables {
		var b strings.Builder
		b.WriteString("UPDATE " + t.MainSQL() + " SET __cf=TRUE,__end='" + c.SourceTimestamp + "',__current=FALSE WHERE __current AND __origin='" + c.Origin + "'")
		if err := wherePKDataEqual(db, &b, c.Column); err != nil {
			return err
		}
		// Run SQL.
		if _, err := tx.Exec(context.TODO(), b.String()); err != nil {
			return err
		}
	}
	return nil
}

func execTruncateData(cat *catalog.Catalog, c *command.Command, tx pgx.Tx, db sqlx.DB) error {
	// Get the transformed tables so that we can propagate the truncate operation.
	tables := cat.DescendantTables(dbx.Table{S: c.SchemaName, T: c.TableName})
	// Note that if the table does not exist, "tables" will be an empty slice and the
	// loop below will not do anything.

	// Mark as not current.
	// TODO Use pgx.Batch
	for _, t := range tables {
		var b strings.Builder
		b.WriteString("UPDATE " + t.MainSQL() + " SET __cf=TRUE,__end='" + c.SourceTimestamp + "',__current=FALSE WHERE __current AND __origin='" + c.Origin + "'")
		// Run SQL.
		if _, err := tx.Exec(context.TODO(), b.String()); err != nil {
			return err
		}
	}
	return nil
}

func wherePKDataEqual(db sqlx.DB, b *strings.Builder, columns []command.CommandColumn) error {
	first := true
	for _, c := range columns {
		if c.PrimaryKey != 0 {
			b.WriteString(" AND")
			if c.DType == command.JSONType {
				b.WriteString(" " + db.IdentiferSQL(c.Name) + "::text=" + encodeSQLData(c.SQLData, c.DType, db) + "::text")
			} else {
				b.WriteString(" " + db.IdentiferSQL(c.Name) + "=" + encodeSQLData(c.SQLData, c.DType, db))
			}
			first = false
		}
	}
	if first {
		return fmt.Errorf("command missing primary key")
	}
	return nil
}

func encodeSQLData(sqldata *string, datatype command.DataType, db sqlx.DB) string {
	if sqldata == nil {
		return "NULL"
	}
	switch datatype {
	case command.TextType, command.JSONType:
		return db.EncodeString(*sqldata)
	case command.DateType, command.TimeType, command.TimetzType, command.TimestampType, command.TimestamptzType, command.UUIDType:
		return "'" + *sqldata + "'"
	case command.IntegerType, command.FloatType, command.NumericType, command.BooleanType:
		return *sqldata
	default:
		log.Error("encoding SQL data: unknown data type: %s", datatype)
		return "(unknown type)"
	}
}

/*func checkRowExistsCurrent(c *command.Command, tx *sql.Tx, history bool) (int64, error) {
	var h string
	if history {
		h = "__"
	}
	var err error
	var pkey []command.CommandColumn = command.PrimaryKeyColumns(c.Column)
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, ""+
		"SELECT __id\n"+
		"    FROM %s\n"+
		"    WHERE __origin = '%s'", util.JoinSchemaTable(c.SchemaName, c.TableName+h), c.Origin)
	var col command.CommandColumn
	for _, col = range pkey {
		_, _ = fmt.Fprintf(&b, " AND\n        %s = %s", col.Name, command.SQLEncodeData(col.Data, col.DType, col.SemanticType))
	}
	if history {
		_, _ = fmt.Fprintf(&b, " AND\n        __current = TRUE")
	}
	_, _ = fmt.Fprintf(&b, ";")
	var q = b.String()
	var id int64
	err = tx.QueryRowContext(context.TODO(), q).Scan(&id)
	if err == nil {
		return id, nil
	}
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return 0, fmt.Errorf("%s:\n%s", err, q)
}
*/
