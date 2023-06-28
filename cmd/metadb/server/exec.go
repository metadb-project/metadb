package server

import (
	"context"
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

func execCommandList(cat *catalog.Catalog, cmdlist *command.CommandList, db sqlx.DB, dp *pgxpool.Pool, source string) error {
	var cmdlisttxns = partitionTxn(cmdlist)
	for i := range cmdlisttxns {
		if len(cmdlisttxns[i].Cmd) == 0 {
			continue
		}
		// exec schema changes
		if err := execCommandSchema(cat, &(cmdlisttxns[i].Cmd[0]), db, source); err != nil {
			return fmt.Errorf("exec command schema: %v", err)
		}
		if err := execCommandAddIndexes(cat, &(cmdlisttxns[i])); err != nil {
			return fmt.Errorf("exec command indexes: %v", err)
		}
		if err := execCommandListData(cat, db, dp, &(cmdlisttxns[i])); err != nil {
			return fmt.Errorf("exec command data: %v", err)
		}
		// log confirmation
		if log.IsLevelTrace() {
			for _, c := range cmdlisttxns[i].Cmd {
				logTraceCommand(&c)
			}
		}
	}
	return nil
}

func execCommandSchema(cat *catalog.Catalog, cmd *command.Command, db sqlx.DB, source string) error {

	if cmd.Op == command.DeleteOp || cmd.Op == command.TruncateOp {
		return nil
	}
	var err error
	var delta *deltaSchema
	if delta, err = findDeltaSchema(cat, cmd); err != nil {
		return fmt.Errorf("schema: %v", err)
	}
	if err = addTable(cmd, db, cat, source); err != nil {
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
		err := alterColumnType(cat, db, tschema, tableName, col.name, command.TextType, 0, false)
		if err != nil {
			return fmt.Errorf("delta schema: %v", err)
		}
	}
	return nil
}

func execCommandAddIndexes(cat *catalog.Catalog, cmdlist *command.CommandList) error {
	commands := cmdlist.Cmd
	for i := range commands {
		// The table associated with delete/truncate operations may not exist, and any
		// needed indexes would have been created anyway with a merge operation.
		if commands[i].Op == command.DeleteOp || commands[i].Op == command.TruncateOp {
			continue
		}
		// Create indexes on primary key columns.
		for _, col := range commands[i].Column {
			if col.PrimaryKey != 0 {
				if err := cat.AddIndexIfNotExists(commands[i].SchemaName, commands[i].TableName, col.Name); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func execCommandListData(cat *catalog.Catalog, db sqlx.DB, dp *pgxpool.Pool, cmdlist *command.CommandList) error {
	tx, err := dp.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer dbx.Rollback(tx)
	// Exec data
	commands := cmdlist.Cmd
	for i := range cmdlist.Cmd {
		// Execute data part of command
		if err = execCommandData(cat, &(commands[i]), tx, db); err != nil {
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

func partitionTxn(cmdlist *command.CommandList) []command.CommandList {
	// cmdlisttxns is a partition of command lists into transactions.
	cmdlisttxns := make([]command.CommandList, 0)
	// newcmdlist is the command list we are currently adding commands to.
	newcmdlist := new(command.CommandList)
	// lastcmd stores the previous command that was examined.
	lastcmd := command.Command{}
	commands := cmdlist.Cmd
	for i := range commands {
		// If a command may require schema changes, end the current transaction and begin
		// a new one.
		if mayRequireSchemaChanges(&(commands[i]), &lastcmd) && len(newcmdlist.Cmd) > 0 {
			cmdlisttxns = append(cmdlisttxns, *newcmdlist)
			newcmdlist = new(command.CommandList)
		}
		newcmdlist.Cmd = append(newcmdlist.Cmd, commands[i])
		if commands[i].Op == command.MergeOp {
			lastcmd = commands[i]
		}
	}
	if len(newcmdlist.Cmd) > 0 {
		cmdlisttxns = append(cmdlisttxns, *newcmdlist)
	}
	return cmdlisttxns
}

func mayRequireSchemaChanges(cmd, lastcmd *command.Command) bool {
	switch {
	case cmd.Op != command.MergeOp:
		return false
	case cmd.SchemaName != lastcmd.SchemaName:
		return true
	case cmd.TableName != lastcmd.TableName:
		return true
	case len(cmd.Column) != len(lastcmd.Column):
		return true
	}
	// If the new command has a column not present in the previous command, or
	// present but different, then schema changes may be required.
	columns := cmd.Column
	for i := range columns {
		lastcol, ok := lastcmd.ColumnMap[columns[i].Name]
		if !ok {
			return true
		}
		switch {
		case columns[i].DType != lastcol.DType:
			return true
		case columns[i].DTypeSize != lastcol.DTypeSize:
			return true
		case columns[i].PrimaryKey != lastcol.PrimaryKey:
			return true
		}
	}
	return false
}

func execCommandData(cat *catalog.Catalog, cmd *command.Command, tx pgx.Tx, db sqlx.DB) error {
	switch cmd.Op {
	case command.MergeOp:
		if err := execMergeData(cmd, tx, db); err != nil {
			return fmt.Errorf("merge: %v", err)
		}
	case command.DeleteOp:
		if err := execDeleteData(cat, cmd, tx, db); err != nil {
			return fmt.Errorf("delete: %v", err)
		}
	case command.TruncateOp:
		if err := execTruncateData(cat, cmd, tx, db); err != nil {
			return fmt.Errorf("truncate: %v", err)
		}
	default:
		return fmt.Errorf("unknown command op: %v", cmd.Op)
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
	unavailColumns := make([]*command.CommandColumn, 0)
	columns := cmd.Column
	for i := range columns {
		if columns[i].Unavailable {
			unavailColumns = append(unavailColumns, &(columns[i]))
		}
	}
	if len(unavailColumns) != 0 {
		var b strings.Builder
		b.WriteString("SELECT ")
		for i := range unavailColumns {
			if i != 0 {
				b.WriteByte(',')
			}
			b.WriteByte('"')
			b.WriteString(unavailColumns[i].Name)
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
		lenColumns := len(unavailColumns)
		dest := make([]any, lenColumns)
		values := make([]any, lenColumns)
		for i := range values {
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
			for i := range unavailColumns {
				if values[i] == nil {
					return fmt.Errorf("nil value in replacing unavailable data")
				}
				s := values[i].(string)
				unavailColumns[i].SQLData = &s
				log.Trace("found current value for unavailable data in table %q, column %q", table, unavailColumns[i].Name)
				break
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
	batch := &pgx.Batch{}
	batch.Queue(b.String())
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
	for i := range columns {
		b.WriteString(",\"")
		b.WriteString(columns[i].Name)
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
	for i := range columns {
		b.WriteString(",")
		b.WriteString(encodeSQLData(columns[i].SQLData, columns[i].DType, db))
	}
	b.WriteByte(')')
	batch.Queue(b.String())
	if err = tx.SendBatch(context.TODO(), batch).Close(); err != nil {
		return fmt.Errorf("update and insert: %v", err)
	}
	return nil
}

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
	columns := cmd.Column
	for i := range columns {
		if columns[i].Unavailable {
			continue
		}
		b.WriteString(" AND \"")
		b.WriteString(columns[i].Name)
		if columns[i].Data == nil {
			b.WriteString("\" IS NULL")
		} else {
			b.WriteString("\"=")
			b.WriteString(encodeSQLData(columns[i].SQLData, columns[i].DType, db))
		}
	}
	b.WriteString(" LIMIT 1")
	rows, err := tx.Query(context.TODO(), b.String())
	if err != nil {
		return false, false, fmt.Errorf("querying for matching current row: %v", err)
	}
	defer rows.Close()
	columnNames := make([]string, 0)
	fields := rows.FieldDescriptions()
	for i := range fields {
		columnNames = append(columnNames, fields[i].Name)
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
	rows.Close()
	if !found {
		return false, false, nil
	}
	// If any extra column values are not NULL, there is no match.
	var cf bool
	for i := range values {
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
		for j := range columns {
			if columnNames[i] == columns[j].Name {
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

func execDeleteData(cat *catalog.Catalog, c *command.Command, tx pgx.Tx, db sqlx.DB) error {
	// Get the transformed tables so that we can propagate the delete operation.
	tables := cat.DescendantTables(dbx.Table{S: c.SchemaName, T: c.TableName})
	// Note that if the table does not exist, "tables" will be an empty slice and the
	// loop below will not do anything.

	// Find matching current record and mark as not current.
	// TODO Use pgx.Batch
	for i := range tables {
		var b strings.Builder
		b.WriteString("UPDATE " + tables[i].MainSQL() + " SET __cf=TRUE,__end='" + c.SourceTimestamp + "',__current=FALSE WHERE __current AND __origin='" + c.Origin + "'")
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
	for i := range tables {
		var b strings.Builder
		b.WriteString("UPDATE " + tables[i].MainSQL() + " SET __cf=TRUE,__end='" + c.SourceTimestamp + "',__current=FALSE WHERE __current AND __origin='" + c.Origin + "'")
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
