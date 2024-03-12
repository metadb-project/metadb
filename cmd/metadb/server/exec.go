package server

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metadb-project/metadb/cmd/internal/uuid"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/dsync"
	"github.com/metadb-project/metadb/cmd/metadb/log"
)

func execCommandList(ctx context.Context, cat *catalog.Catalog, cmdlist *list.List, dp *pgxpool.Pool, source string, syncMode dsync.Mode, dedup *log.MessageSet) error {
	if cmdlist.Len() == 0 {
		return nil
	}
	ebuf := &execbuffer{
		ctx:       ctx,
		dp:        dp,
		syncIDs:   make(map[dbx.Table][][]any),
		mergeData: make(map[dbx.Table][][]string),
		syncMode:  syncMode,
	}
	txnTime := time.Now()
	for e := cmdlist.Front(); e != nil; e = e.Next() {
		cmd := e.Value.(*command.Command)
		if log.IsLevelTrace() {
			logTraceCommand(cmd)
		}
		if err := execCommand(ebuf, cat, cmd, source, syncMode, dedup); err != nil {
			return fmt.Errorf("exec command: %v", err)
		}
	}
	if err := ebuf.flush(); err != nil {
		return fmt.Errorf("exec command list: %v", err)
	}
	log.Trace("=================================================================")
	log.Trace("exec: %d records %s", cmdlist.Len(), fmt.Sprintf("[%.4f s]", time.Since(txnTime).Seconds()))
	log.Trace("=================================================================")
	return nil
}

func execCommand(ebuf *execbuffer, cat *catalog.Catalog, cmd *command.Command, source string, syncMode dsync.Mode, dedup *log.MessageSet) error {
	// Make schema changes if needed by the command.
	if cmd.Op == command.MergeOp {
		table := &dbx.Table{Schema: cmd.SchemaName, Table: cmd.TableName}
		delta, err := findDeltaSchema(cat, cmd, table)
		if err != nil {
			return fmt.Errorf("finding schema delta: %v", err)
		}
		if err = addTable(ebuf, cmd, cat, table, source); err != nil {
			return fmt.Errorf("schema: %v", err)
		}
		if err = addPartition(ebuf, cat, cmd); err != nil {
			return fmt.Errorf("schema: %v", err)
		}
		// Note that execDeltaSchema() may adjust data types in cmd.
		if err = execDeltaSchema(ebuf, cat, cmd, delta, table); err != nil {
			return fmt.Errorf("schema: %v", err)
		}
		// Ensure indexes are created on primary key columns.
		for _, col := range cmd.Column {
			if col.PrimaryKey != 0 {
				column := &dbx.Column{Schema: table.Schema, Table: table.Table, Column: col.Name}
				if cat.IndexExists(column) {
					continue
				}
				if err = ebuf.flush(); err != nil {
					return fmt.Errorf("creating indexes: %v", err)
				}
				if err = cat.AddIndex(column); err != nil {
					return err
				}
			}
		}
	}
	if err := execCommandData(ebuf, cat, cmd, syncMode, dedup); err != nil {
		return fmt.Errorf("exec data: %v", err)
	}
	return nil
}

func findDeltaSchema(cat *catalog.Catalog, cmd *command.Command, table *dbx.Table) (*deltaSchema, error) {
	schema1, err := selectTableSchema(cat, table)
	if err != nil {
		return nil, err
	}
	schema2 := tableSchemaFromCommand(cmd)
	delta := new(deltaSchema)
	for i := range schema2.Column {
		col1 := getColumnSchema(schema1, schema2.Column[i].Name)
		findDeltaColumnSchema(col1, &(schema2.Column[i]), delta)
	}
	// findDeltaPrimaryKey()
	return delta, nil
}

func addTable(ebuf *execbuffer, cmd *command.Command, cat *catalog.Catalog, table *dbx.Table, source string) error {
	if cat.TableExists(table) {
		return nil
	}
	if err := ebuf.flush(); err != nil {
		return fmt.Errorf("creating table %q : %v", table, err)
	}
	parentTable := dbx.Table{Schema: cmd.ParentTable.Schema, Table: cmd.ParentTable.Table}
	err := cat.CreateNewTable(table, cmd.Transformed, &parentTable, source)
	if err != nil {
		return fmt.Errorf("creating table %q: %v", table, err)
	}
	return nil
}

func execDeltaSchema(ebuf *execbuffer, cat *catalog.Catalog, cmd *command.Command, delta *deltaSchema, table *dbx.Table) error {
	//if len(delta.column) == 0 {
	//        log.Trace("table %s: no schema changes", util.JoinSchemaTable(tschema, tableName))
	//}
	for _, col := range delta.column {
		// Is this a new column (as opposed to a modification)?
		if col.newColumn {
			dtypesql := command.DataTypeToSQL(col.newType, col.newTypeSize)
			log.Trace("table %s.%s: new column: %s %s", table.Schema, table.Table, col.name, dtypesql)
			if err := ebuf.flush(); err != nil {
				return fmt.Errorf("delta schema: adding column %q in table %q: %v", col.name, table, err)
			}
			if err := cat.AddColumn(table, col.name, col.newType, col.newTypeSize); err != nil {
				return fmt.Errorf("delta schema: adding column %q in table %q: %v", col.name, table, err)
			}
			continue
		}
		// If the type is changing from text to another type, keep the type as text and
		// let the executor cast the data. This is to prevent poorly typed JSON fields
		// from causing runaway type changes.
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
				return fmt.Errorf("delta schema: internal error: column not found in command: %s.%s (%s)", table.Schema, table.Table, col.name)
			}
			if typeSize <= col.oldTypeSize {
				continue
			}
			// Change the delta column type as well so that column size can be adjusted below
			// if needed.
			col.newType = command.TextType
			col.newTypeSize = typeSize
		}

		// If this is a change from a UUID to text type, it may be that the UUID type was
		// inferred from a text type in the source.  For this reason we will prefer to
		// retain the UUID type, unless the new data is not a valid UUID.
		if col.oldType == command.UUIDType && col.newType == command.TextType {
			if col.newData == nil {
				continue
			}
			newDataStr, ok := col.newData.(string)
			if ok && uuid.IsUUID(newDataStr) {
				continue
			}
		}

		// If both the old and new types are IntegerType, change the column type to
		// handle the larger size.
		if col.oldType == command.IntegerType && col.newType == command.IntegerType {
			if err := ebuf.flush(); err != nil {
				return fmt.Errorf("delta schema: altering column %q (%q) type to %v: %v", table, col.name, command.IntegerType, err)
			}
			if err := alterColumnType(ebuf.dp, cat, table, col.name, command.IntegerType, col.newTypeSize, false); err != nil {
				return fmt.Errorf("delta schema: altering column %q (%q) type to %v: %v", table, col.name, command.IntegerType, err)
			}
			continue
		}

		// If both the old and new types are FloatType, change the column type to handle
		// the larger size.
		if col.oldType == command.FloatType && col.newType == command.FloatType {
			if err := ebuf.flush(); err != nil {
				return fmt.Errorf("altering column %q (%q) type to %v: %v", table, col.name, command.FloatType, err)
			}
			if err := alterColumnType(ebuf.dp, cat, table, col.name, command.FloatType, col.newTypeSize, false); err != nil {
				return fmt.Errorf("delta schema: altering column %q (%q) type to %v: %v", table, col.name, command.FloatType, err)
			}
			continue
		}

		// If this is a change from an integer to float type, the column type can be
		// changed using a cast.
		if col.oldType == command.IntegerType && col.newType == command.FloatType {
			if err := ebuf.flush(); err != nil {
				return fmt.Errorf("altering column %q (%q) type to %v: %v", table, col.name, command.FloatType, err)
			}
			if err := alterColumnType(ebuf.dp, cat, table, col.name, command.FloatType, col.newTypeSize, false); err != nil {
				return fmt.Errorf("delta schema: altering column %q (%q) type to %v: %v", table, col.name, command.FloatType, err)
			}
			continue
		}

		// If this is a change from an integer or float to numeric type, the column type
		// can be changed using a cast.
		if (col.oldType == command.IntegerType || col.oldType == command.FloatType) && col.newType == command.NumericType {
			if err := ebuf.flush(); err != nil {
				return fmt.Errorf("altering column %q (%q) type to %v: %v", table, col.name, command.NumericType, err)
			}
			if err := alterColumnType(ebuf.dp, cat, table, col.name, command.NumericType, 0, false); err != nil {
				return fmt.Errorf("delta schema: altering column %q (%q) type to %v: %v", table, col.name, command.NumericType, err)
			}
			continue
		}

		// If this is a change from a float to integer type, cast the column to the
		// numeric type.
		if col.oldType == command.FloatType && col.newType == command.IntegerType {
			if err := ebuf.flush(); err != nil {
				return fmt.Errorf("altering column %q (%q) type to %v: %v", table, col.name, command.NumericType, err)
			}
			if err := alterColumnType(ebuf.dp, cat, table, col.name, command.NumericType, 0, false); err != nil {
				return fmt.Errorf("delta schema: altering column %q (%q) type to %v: %v", table, col.name, command.NumericType, err)
			}
			continue
		}

		// Prevent conversion from numeric to integer or float type.
		if col.oldType == command.NumericType && (col.newType == command.IntegerType || col.newType == command.FloatType) {
			continue
		}

		// If not a compatible change, adjust new type to text in all cases, unless it is
		// already text.
		if col.oldType != command.TextType {
			if err := ebuf.flush(); err != nil {
				return fmt.Errorf("altering column %q (%q) type to %v: %v", table, col.name, command.TextType, err)
			}
			for _, d := range delta.column {
				log.Trace("COLUMN: %#v", d)
			}
			if err := alterColumnType(ebuf.dp, cat, table, col.name, command.TextType, 0, false); err != nil {
				return fmt.Errorf("delta schema: altering column %q (%q) type to %v: %v", table, col.name, command.TextType, err)
			}
		}
	}
	return nil
}

func execCommandData(ebuf *execbuffer, cat *catalog.Catalog, cmd *command.Command, syncMode dsync.Mode, dedup *log.MessageSet) error {
	switch cmd.Op {
	case command.MergeOp:
		if err := execMergeData(ebuf, cmd, syncMode, dedup); err != nil {
			return fmt.Errorf("merge: %v", err)
		}
	case command.DeleteOp:
		if err := execDeleteData(ebuf, cat, cmd); err != nil {
			return fmt.Errorf("delete: %v", err)
		}
	case command.TruncateOp:
		if err := execTruncateData(ebuf, cat, cmd); err != nil {
			return fmt.Errorf("truncate: %v", err)
		}
	default:
		return fmt.Errorf("unknown command op: %v", cmd.Op)
	}
	return nil
}

// execMergeData executes a merge command in the database.
func execMergeData(ebuf *execbuffer, cmd *command.Command, syncMode dsync.Mode, dedup *log.MessageSet) error {
	table := &dbx.Table{Schema: cmd.SchemaName, Table: cmd.TableName}
	// Check if the current record (if any) is identical to the new one.  If so, we
	// can avoid making any changes in the database.
	var id int64
	ident, id, err := isCurrentIdentical(ebuf.ctx, cmd, ebuf.dp, table)
	if err != nil {
		return fmt.Errorf("matcher: %v", err)
	}
	if ident {
		log.Trace("new command matches current record")
		// If resync mode, write __id to sync table.
		if syncMode == dsync.Resync {
			ebuf.queueSyncID(table, id)
		}
		return nil
	}
	primaryKeyFilter := wherePKDataEqualSQL(cmd.Column)
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
		b.WriteString(table.Schema)
		b.WriteString("\".\"")
		b.WriteString(table.Table)
		b.WriteString("\" WHERE __origin='")
		b.WriteString(cmd.Origin)
		b.WriteString("'")
		b.WriteString(primaryKeyFilter)
		b.WriteString(" LIMIT 1")
		var rows pgx.Rows
		rows, err = ebuf.dp.Query(context.TODO(), b.String())
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
		for rows.Next() {
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
			msg := fmt.Sprintf("no current value for unavailable data in table %q", table)
			if dedup.Insert(msg) {
				log.Warning("%s", msg)
			}
		} else {
			for i := range unavailColumns {
				if values[i] == nil {
					msg := fmt.Sprintf("nil value in replacing unavailable data in table %q", table)
					if dedup.Insert(msg) {
						log.Warning("%s", msg)
					}
					continue
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
	b.WriteString(table.Schema)
	b.WriteString("\".\"")
	b.WriteString(table.Table)
	b.WriteString("__\" SET __end='")
	b.WriteString(cmd.SourceTimestamp)
	b.WriteString("',__current='f'")
	b.WriteString(" WHERE __current AND __origin='")
	b.WriteString(cmd.Origin)
	b.WriteByte('\'')
	b.WriteString(primaryKeyFilter)
	update := b.String()
	// Insert the new row.
	b.Reset()
	b.WriteString("INSERT INTO \"")
	b.WriteString(table.Schema)
	b.WriteString("\".\"")
	b.WriteString(table.Table)
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
		encodeSQLData(&b, columns[i].SQLData, columns[i].DType)
	}
	b.WriteString(") RETURNING __id")
	insert := b.String()
	ebuf.queueMergeData(table, &update, &insert)
	return nil
}

// isCurrentIdentical looks for an identical row in the current table.
func isCurrentIdentical(ctx context.Context, cmd *command.Command, tx *pgxpool.Pool, table *dbx.Table) (bool, int64, error) {
	// Match on all columns, except "unavailable" columns (which indicates a column
	// did not change and we can assume it matches).
	var b strings.Builder
	b.WriteString("SELECT * FROM \"")
	b.WriteString(table.Schema)
	b.WriteString("\".\"")
	b.WriteString(table.Table)
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
			encodeSQLData(&b, columns[i].SQLData, columns[i].DType)
		}
	}
	b.WriteString(" LIMIT 1")
	rows, err := tx.Query(ctx, b.String())
	if err != nil {
		return false, 0, fmt.Errorf("querying for matching current row: %v", err)
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
	for rows.Next() {
		found = true
		if err = rows.Scan(dest...); err != nil {
			return false, 0, fmt.Errorf("scanning row values: %v", err)
		}
	}
	if err = rows.Err(); err != nil {
		return false, 0, fmt.Errorf("reading matching current row: %v", err)
	}
	rows.Close()
	if !found {
		return false, 0, nil
	}
	// If any extra column values are not NULL, there is no match.
	columnMap := buildColumnMap(cmd.Column)
	var id int64
	for i := range values {
		if columnNames[i] == "__id" {
			var ok bool
			id, ok = values[i].(int64)
			if !ok {
				return false, 0, fmt.Errorf("error in type assertion of \"__id\" to int64")
			}
			continue
		}
		if strings.HasPrefix(columnNames[i], "__") {
			continue
		}
		_, ok := columnMap[columnNames[i]]
		if ok {
			continue
		}
		// This is an extra column.
		if values[i] != nil {
			return false, 0, nil
		}
	}
	// Otherwise we have found a match.
	return true, id, nil
}

func buildColumnMap(columns []command.CommandColumn) map[string]*command.CommandColumn {
	m := make(map[string]*command.CommandColumn)
	for i := range columns {
		m[columns[i].Name] = &(columns[i])
	}
	return m
}

func execDeleteData(ebuf *execbuffer, cat *catalog.Catalog, cmd *command.Command) error {
	// Flush buffer before deletion, to prevent a previous merge with the same tuple
	// ID from being applied later.
	if err := ebuf.flush(); err != nil {
		return fmt.Errorf("exec delete data: %v", err)
	}
	primaryKeyFilter := wherePKDataEqualSQL(cmd.Column)
	// Find matching current records in table and descendants, and mark as not current.
	batch := pgx.Batch{}
	cat.TraverseDescendantTables(dbx.Table{Schema: cmd.SchemaName, Table: cmd.TableName},
		func(table dbx.Table) {
			batch.Queue("UPDATE " + table.MainSQL() +
				" SET __end='" + cmd.SourceTimestamp + "',__current=FALSE WHERE __current AND __origin='" +
				cmd.Origin + "'" + primaryKeyFilter)
		})
	if err := ebuf.dp.SendBatch(ebuf.ctx, &batch).Close(); err != nil {
		return fmt.Errorf("exec delete data: %v", err)
	}
	return nil
}

func wherePKDataEqualSQL(columns []command.CommandColumn) string {
	var b strings.Builder
	for _, c := range columns {
		if c.PrimaryKey != 0 {
			b.WriteString(" AND")
			if c.DType == command.JSONType {
				b.WriteString(" \"")
				b.WriteString(c.Name)
				b.WriteString("\"::text=")
				encodeSQLData(&b, c.SQLData, c.DType)
				b.WriteString("::text")
			} else {
				b.WriteString(" \"")
				b.WriteString(c.Name)
				b.WriteString("\"=")
				encodeSQLData(&b, c.SQLData, c.DType)
			}
		}
	}
	return b.String()
}

func encodeSQLData(b *strings.Builder, sqldata *string, datatype command.DataType) {
	if sqldata == nil {
		b.WriteString("NULL")
		return
	}
	switch datatype {
	case command.TextType, command.JSONType:
		dbx.EncodeString(b, *sqldata)
	case command.UUIDType, command.DateType, command.TimeType, command.TimetzType, command.TimestampType, command.TimestamptzType:
		b.WriteByte('\'')
		b.WriteString(*sqldata)
		b.WriteByte('\'')
	case command.IntegerType, command.FloatType, command.NumericType, command.BooleanType:
		b.WriteString(*sqldata)
	default:
		log.Error("encoding SQL data: unknown data type: %s", datatype)
		b.WriteString("(unknown type)")
	}
}

func execTruncateData(ebuf *execbuffer, cat *catalog.Catalog, cmd *command.Command) error {
	// Flush buffer before truncation, to prevent a previous merge in the same table
	// from being applied later.
	if err := ebuf.flush(); err != nil {
		return fmt.Errorf("exec truncate data: %v", err)
	}
	// Find all current records in table and descendants, and mark as not current.
	batch := pgx.Batch{}
	cat.TraverseDescendantTables(dbx.Table{Schema: cmd.SchemaName, Table: cmd.TableName},
		func(table dbx.Table) {
			batch.Queue("UPDATE " + table.MainSQL() + " SET __end='" +
				cmd.SourceTimestamp + "',__current=FALSE WHERE __current AND __origin='" + cmd.Origin + "'")
		})
	if err := ebuf.dp.SendBatch(ebuf.ctx, &batch).Close(); err != nil {
		return fmt.Errorf("exec truncate data: %v", err)
	}
	return nil
}
