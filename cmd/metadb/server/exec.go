package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/cache"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func execCommandList(cl *command.CommandList, db *sql.DB, track *cache.Track, schema *cache.Schema, database *sysdb.DatabaseConnector) error {
	var clt []command.CommandList = partitionTxn(cl)
	for _, cc := range clt {
		if len(cc.Cmd) == 0 {
			continue
		}
		// exec schema changes
		if err := execCommandSchema(&cc.Cmd[0], db, track, schema, database); err != nil {
			return err
		}
		// begin txn
		tx, err := sqlx.MakeTx(db)
		if err != nil {
			return fmt.Errorf("exec: start transaction: %s", err)
		}
		defer tx.Rollback()
		// exec data
		for _, c := range cc.Cmd {
			if err := execCommandData(&c, tx); err != nil {
				return fmt.Errorf("%s\n%v", err, c)
			}
		}
		// commit txn
		log.Trace("commit txn")
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("exec: commit transaction: %s", err)
		}
		// log confirmation
		for _, c := range cc.Cmd {
			logDebugCommand(&c)
		}
	}
	return nil
}

func partitionTxn(cl *command.CommandList) []command.CommandList {
	var clt []command.CommandList
	newcl := new(command.CommandList)
	var c, lastc command.Command
	for _, c = range cl.Cmd {
		if requiresSchemaChanges(&c, &lastc) {
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

func requiresSchemaChanges(c, o *command.Command) bool {
	if c.Op == command.DeleteOp && o.Op == command.DeleteOp {
		return false
	}
	if c.Op != o.Op || c.SchemaName != o.SchemaName || c.TableName != o.TableName {
		return true
	}
	for i, col := range c.Column {
		if col.Name != o.Column[i].Name || col.DType != o.Column[i].DType || col.DTypeSize != o.Column[i].DTypeSize || col.SemanticType != o.Column[i].SemanticType || col.PrimaryKey != o.Column[i].PrimaryKey {
			return true
		}
	}
	return false
}

func execCommandSchema(c *command.Command, db *sql.DB, track *cache.Track, schema *cache.Schema, database *sysdb.DatabaseConnector) error {
	if c.Op == command.DeleteOp {
		return nil
	}
	var err error
	var delta *deltaSchema
	if delta, err = findDeltaSchema(c, schema); err != nil {
		return err
	}
	// TODO can we skip adding the table if we confirm it in sysdb?
	if err = addTable(&sqlx.Table{c.SchemaName, c.TableName}, &sqlx.DB{DB: db}, track, database); err != nil {
		return err
	}
	if err = execDeltaSchema(delta, c.SchemaName, c.TableName, db, schema); err != nil {
		return err
	}
	return nil
}

func execDeltaSchema(delta *deltaSchema, tschema string, tableName string, db *sql.DB, schema *cache.Schema) error {
	var err error
	var col deltaColumnSchema
	//if len(delta.column) == 0 {
	//        log.Trace("table %s: no schema changes", util.JoinSchemaTable(tschema, tableName))
	//}
	for _, col = range delta.column {
		// Is this a new column (as opposed to a modification)?
		if col.newColumn {
			log.Trace("table %s: new column: %s %s", util.JoinSchemaTable(tschema, tableName), col.name, command.DataTypeToSQL(col.newType, col.newTypeSize))
			t := &sqlx.Table{Schema: tschema, Table: tableName}
			if err = addColumn(t, col.name, col.newType, col.newTypeSize, db, schema); err != nil {
				return err
			}
			continue
		}
		// If the types are the same and they are varchar, both PostgreSQL and
		// Redshift can alter the column in place
		if col.oldType == col.newType && col.oldType == command.VarcharType {
			log.Trace("table %s: alter column: %s %s", util.JoinSchemaTable(tschema, tableName), col.name, command.DataTypeToSQL(col.newType, col.newTypeSize))
			if err = alterColumnVarcharSize(&sqlx.Table{tschema, tableName}, col.name, col.newType, col.newTypeSize, db, schema); err != nil {
				return err
			}
			continue
		}
		// Otherwise we have a completely new type
		log.Trace("table %s: rename column %s", util.JoinSchemaTable(tschema, tableName), col.name)
		if err = renameColumnOldType(&sqlx.Table{tschema, tableName}, col.name, col.newType, col.newTypeSize, db, schema); err != nil {
			return err
		}
		log.Trace("table %s: new column %s %s", util.JoinSchemaTable(tschema, tableName), col.name, command.DataTypeToSQL(col.newType, col.newTypeSize))
		t := &sqlx.Table{Schema: tschema, Table: tableName}
		if err = addColumn(t, col.name, col.newType, col.newTypeSize, db, schema); err != nil {
			return err
		}
	}
	return nil
}

func execCommandData(c *command.Command, tx *sql.Tx) error {
	switch c.Op {
	case command.MergeOp:
		return execMergeData(c, tx)
	case command.DeleteOp:
		return execDeleteData(c, tx)
	default:
		return fmt.Errorf("unknown command op: %v", c.Op)
	}
}

func execMergeData(c *command.Command, tx *sql.Tx) error {
	t := sqlx.Table{Schema: c.SchemaName, Table: c.TableName}
	// Check if current record is identical
	ident, id, cf, err := isCurrentIdentical(c, tx, &t)
	if err != nil {
		return err
	}
	if ident {
		if cf == "false" {
			return updateRowCF(c, tx, &t, id, false)
		}
		return updateRowCF(c, tx, &t, id, true)
	}
	// current table
	// delete the record
	var b strings.Builder
	if id != "" {
		b.WriteString("DELETE FROM " + t.SQL() + " WHERE __id='" + id + "';")
	}
	// insert new record
	b.WriteString("INSERT INTO " + t.SQL() + "(__start")
	if c.Origin != "" {
		b.WriteString(",__origin")
	}
	for _, c := range c.Column {
		b.WriteString(",\"" + c.Name + "\"")
	}
	b.WriteString(")VALUES('" + c.SourceTimestamp + "'")
	if c.Origin != "" {
		b.WriteString(",'" + c.Origin + "'")
	}
	for _, c := range c.Column {
		b.WriteString("," + c.EncodedData)
	}
	b.WriteString(");")
	// history table
	// select matching current record in history table and mark as not current
	b.WriteString("UPDATE " + t.History().SQL() + " SET __current=FALSE,__end='" + c.SourceTimestamp + "' WHERE __id=(SELECT __id FROM " + t.History().SQL() + " WHERE __origin='" + c.Origin + "'")
	if err := wherePKDataEqual(&b, c.Column); err != nil {
		return err
	}
	b.WriteString(" AND __current LIMIT 1);")
	// insert new record
	b.WriteString("INSERT INTO " + t.History().SQL() + "(__current,__start,__end")
	if c.Origin != "" {
		b.WriteString(",__origin")
	}
	for _, c := range c.Column {
		b.WriteString(",\"" + c.Name + "\"")
	}
	b.WriteString(")VALUES(TRUE,'" + c.SourceTimestamp + "','9999-12-31 00:00:00Z'")
	if c.Origin != "" {
		b.WriteString(",'" + c.Origin + "'")
	}
	for _, c := range c.Column {
		b.WriteString("," + c.EncodedData)
	}
	b.WriteString(");")
	if _, err := tx.ExecContext(context.TODO(), b.String()); err != nil {
		return err
	}
	return nil
}

func updateRowCF(c *command.Command, tx *sql.Tx, t *sqlx.Table, id string, historyOnly bool) error {
	var b strings.Builder
	if !historyOnly {
		// current table
		b.WriteString("UPDATE " + t.SQL() + " SET __cf=TRUE WHERE __id='" + id + "';")
	}
	// history table
	// select matching current record in history table
	b.WriteString("UPDATE " + t.History().SQL() + " SET __cf=TRUE WHERE __id=(SELECT __id FROM " + t.History().SQL() + " WHERE __origin='" + c.Origin + "'")
	if err := wherePKDataEqual(&b, c.Column); err != nil {
		return err
	}
	b.WriteString(" AND NOT __cf AND __current LIMIT 1);")
	if _, err := tx.ExecContext(context.TODO(), b.String()); err != nil {
		return err
	}
	return nil
}

func isCurrentIdentical(c *command.Command, tx *sql.Tx, t *sqlx.Table) (bool, string, string, error) {
	var b strings.Builder
	b.WriteString("SELECT * FROM " + t.SQL() + " WHERE __origin='" + c.Origin + "'")
	if err := wherePKDataEqual(&b, c.Column); err != nil {
		return false, "", "", err
	}
	b.WriteString(" LIMIT 1")
	rows, err := tx.QueryContext(context.TODO(), b.String())
	if err != nil {
		return false, "", "", err
	}
	cols, err := rows.Columns()
	if err != nil {
		return false, "", "", err
	}
	ptrs := make([]interface{}, len(cols))
	results := make([][]byte, len(cols))
	for i := range results {
		ptrs[i] = &results[i]
	}
	defer rows.Close()
	var id, cf string
	attrs := make(map[string]*string)
	if rows.Next() {
		if err = rows.Scan(ptrs...); err != nil {
			return false, "", "", err
		}
		for i, r := range results {
			if r != nil {
				attr := cols[i]
				val := string(r)
				switch attr {
				case "__id":
					id = val
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
		return false, "", "", nil
	}
	for _, col := range c.Column {
		var cdata interface{}
		var ddata *string
		var cdatas, ddatas string
		cdata = col.Data
		if cdata != nil {
			cdatas = fmt.Sprintf("%v", cdata)
		}
		ddata = attrs[col.Name]
		if ddata != nil {
			ddatas = *ddata
		}
		if (cdata == nil && ddata != nil) || (cdata != nil && ddata == nil) {
			return false, id, cf, nil
		}
		if cdata != nil && ddata != nil && cdatas != ddatas {
			return false, id, cf, nil
		}
		delete(attrs, col.Name)
	}
	for _, v := range attrs {
		if v != nil {
			return false, id, cf, nil
		}
	}
	return true, id, cf, nil
}

func execDeleteData(c *command.Command, tx *sql.Tx) error {
	t := sqlx.Table{Schema: c.SchemaName, Table: c.TableName}
	// current table
	// delete the record
	var b strings.Builder
	b.WriteString("DELETE FROM " + t.SQL() + " WHERE __id=(SELECT __id FROM " + t.SQL() + " WHERE __origin='" + c.Origin + "'")
	if err := wherePKDataEqual(&b, c.Column); err != nil {
		return err
	}
	b.WriteString(" LIMIT 1);")
	// history table
	// subselect matching current record in history table and mark as not current
	b.WriteString("UPDATE " + t.History().SQL() + " SET __current=FALSE,__end='" + c.SourceTimestamp + "' WHERE __id=(SELECT __id FROM " + t.History().SQL() + " WHERE __origin='" + c.Origin + "'")
	if err := wherePKDataEqual(&b, c.Column); err != nil {
		return err
	}
	b.WriteString(" AND __current LIMIT 1);")
	_, err := tx.ExecContext(context.TODO(), b.String())
	if err != nil {
		return err
	}
	return nil
}

func wherePKDataEqual(b *strings.Builder, columns []command.CommandColumn) error {
	first := true
	for _, c := range columns {
		if c.PrimaryKey != 0 {
			b.WriteString(" AND")
			if c.DType == command.JSONType {
				b.WriteString(" " + c.Name + "::text=" + c.EncodedData + "::text")
			} else {
				b.WriteString(" " + c.Name + "=" + c.EncodedData)
			}
			first = false
		}
	}
	if first {
		return fmt.Errorf("command missing primary key")
	}
	return nil
}

func checkRowExistsCurrent(c *command.Command, tx *sql.Tx, history bool) (int64, error) {
	var h string
	if history {
		h = "__"
	}
	var err error
	var pkey []command.CommandColumn = primaryKeyColumns(c.Column)
	var b strings.Builder
	fmt.Fprintf(&b, ""+
		"SELECT __id\n"+
		"    FROM %s\n"+
		"    WHERE __origin = '%s'", util.JoinSchemaTable(c.SchemaName, c.TableName+h), c.Origin)
	var col command.CommandColumn
	for _, col = range pkey {
		fmt.Fprintf(&b, " AND\n        %s = %s", col.Name, command.SQLEncodeData(col.Data, col.DType, col.SemanticType))
	}
	if history {
		fmt.Fprintf(&b, " AND\n        __current = TRUE")
	}
	fmt.Fprintf(&b, ";")
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
