package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

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
	now := time.Now().Format(time.RFC3339)
	// current table
	// subselect a single record
	var b strings.Builder
	b.WriteString("(SELECT __id FROM " + t.SQL() + " WHERE __origin='" + c.Origin + "'")
	if ok := wherePKDataEqual(&b, c.Column); !ok {
		return nil
	}
	b.WriteString(" LIMIT 1)")
	// delete the record
	_, err := tx.ExecContext(context.TODO(), "DELETE FROM "+t.SQL()+" WHERE __id="+b.String())
	if err != nil {
		return err
	}
	// insert new record
	b.Reset()
	b.WriteString("INSERT INTO " + t.SQL() + " (__start,__origin")
	for _, c := range c.Column {
		b.WriteString(",\"" + c.Name + "\"")
	}
	b.WriteString(") VALUES ('" + now + "','" + c.Origin + "'")
	for _, c := range c.Column {
		b.WriteString("," + c.EncodedData)
	}
	b.WriteString(")")
	if _, err = tx.ExecContext(context.TODO(), b.String()); err != nil {
		return err
	}
	// history table
	// subselect matching current record in history table
	b.Reset()
	b.WriteString("(SELECT __id FROM " + t.History().SQL() + " WHERE __origin='" + c.Origin + "'")
	if ok := wherePKDataEqual(&b, c.Column); !ok {
		return nil
	}
	b.WriteString(" AND __current LIMIT 1)")
	// mark as not current
	_, err = tx.ExecContext(context.TODO(),
		"UPDATE "+t.History().SQL()+" SET __current=FALSE,__end=$1 WHERE __id="+b.String(), now)
	if err != nil {
		return err
	}
	// insert new record
	b.Reset()
	b.WriteString("INSERT INTO " + t.History().SQL() + " (__current,__start,__end,__origin")
	for _, c := range c.Column {
		b.WriteString(",\"" + c.Name + "\"")
	}
	b.WriteString(") VALUES (TRUE,'" + now + "','9999-12-31 00:00:00-00','" + c.Origin + "'")
	for _, c := range c.Column {
		b.WriteString("," + c.EncodedData)
	}
	b.WriteString(")")
	if _, err = tx.ExecContext(context.TODO(), b.String()); err != nil {
		return err
	}
	return nil
}

func execDeleteData(c *command.Command, tx *sql.Tx) error {
	t := sqlx.Table{Schema: c.SchemaName, Table: c.TableName}
	now := time.Now().Format(time.RFC3339)
	// current table
	// subselect a single record
	var b strings.Builder
	b.WriteString("(SELECT __id FROM " + t.SQL() + " WHERE __origin='" + c.Origin + "'")
	if ok := wherePKDataEqual(&b, c.Column); !ok {
		return nil
	}
	b.WriteString(" LIMIT 1)")
	// delete the record
	_, err := tx.ExecContext(context.TODO(), "DELETE FROM "+t.SQL()+" WHERE __id="+b.String())
	if err != nil {
		return err
	}
	// history table
	// subselect matching current record in history table
	b.Reset()
	b.WriteString("(SELECT __id FROM " + t.History().SQL() + " WHERE __origin='" + c.Origin + "'")
	if ok := wherePKDataEqual(&b, c.Column); !ok {
		return nil
	}
	b.WriteString(" AND __current LIMIT 1)")
	// mark as not current
	_, err = tx.ExecContext(context.TODO(),
		"UPDATE "+t.History().SQL()+" SET __current=FALSE,__end=$1 WHERE __id="+b.String(), now)
	if err != nil {
		return err
	}
	return nil
}

func wherePKDataEqual(b *strings.Builder, columns []command.CommandColumn) bool {
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
		log.Error("command missing primary key")
		return false
	}
	return true
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

// func commandPrimaryKey(column []CommandColumn) []ColumnSchema {
// 	var pk []ColumnSchema
// 	var c CommandColumn
// 	for _, c = range column {
// 		if c.PrimaryKey > 0 {
// 			pk = append(
// 				pk,
// 				ColumnSchema{
// 					Name:       c.Name,
// 					Type:       c.Type,
// 					TypeSize:   c.TypeSize,
// 					PrimaryKey: c.PrimaryKey,
// 				})
// 		}
// 	}
// 	sort.Slice(pkey, func(i, j int) bool {
// 		return pkey[i].PrimaryKey > pkey[j].PrimaryKey
// 	})
// 	return pk
// }

// func execCommandTxn(txn *CommandTxn, db *sql.DB) error {
// 	var err error
// 	if err = execCommandSchema(&txn.Cmd[0], svr); err != nil {
// 		return err
// 	}
// 	var cmd Command
// 	for _, cmd = range txn.Cmd {
// 		_ = cmd
// 		if err = execCommandData(&cmd, svr); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }
