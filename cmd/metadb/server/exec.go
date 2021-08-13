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
	if c.Op == command.DeleteOp {
		return execDeleteData(c, tx)
	}
	var err error
	var timeNow string = time.Now().Format(time.RFC3339)
	// Check if a current version of the row exists
	var idc int64
	if idc, err = checkRowExistsCurrent(c, tx, false); err != nil {
		return err
	}
	var idh int64
	if idh, err = checkRowExistsCurrent(c, tx, true); err != nil {
		return err
	}
	// If exists, delete row in current table
	if idc != 0 {
		var q = fmt.Sprintf(""+
			"DELETE FROM %s\n"+
			"    WHERE __id = %d;", util.JoinSchemaTable(c.SchemaName, c.TableName), idc)
		if _, err = tx.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("%s:\n%s", err, q)
		}
	}
	// If exists, set __current to false in history table
	if idh != 0 {
		var q = fmt.Sprintf(""+
			"UPDATE %s\n"+
			"    SET __current = FALSE,\n"+
			"        __end = '%s'\n"+
			"    WHERE __id = %d;", util.JoinSchemaTable(c.SchemaName, c.TableName+"__"), timeNow, idh)
		if _, err = tx.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("%s:\n%s", err, q)
		}
	}
	// Insert into current table
	var b strings.Builder
	fmt.Fprintf(&b, ""+
		"INSERT INTO %s (\n"+
		"        __start,\n"+
		"        __origin", util.JoinSchemaTable(c.SchemaName, c.TableName))
	var col command.CommandColumn
	for _, col = range c.Column {
		fmt.Fprintf(&b, ",\n        \"%s\"", col.Name)
	}
	fmt.Fprintf(&b, "\n"+
		"    ) VALUES (\n"+
		"        '%s',\n"+
		"        '%s'", timeNow, c.Origin)
	for _, col = range c.Column {
		//fmt.Fprintf(&b, ",\n        %s", command.SQLEncodeData(col.Data, col.DType))
		fmt.Fprintf(&b, ",\n        %s", col.EncodedData)
	}
	fmt.Fprintf(&b, "\n    );")
	var q = b.String()
	if _, err = tx.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	// Insert into history table
	b.Reset()
	fmt.Fprintf(&b, ""+
		"INSERT INTO %s (\n"+
		"        __current,\n"+
		"        __start,\n"+
		"        __end,\n"+
		"        __origin", util.JoinSchemaTable(c.SchemaName, c.TableName+"__"))
	for _, col = range c.Column {
		fmt.Fprintf(&b, ",\n        \"%s\"", col.Name)
	}
	fmt.Fprintf(&b, "\n"+
		"    ) VALUES (\n"+
		"        TRUE,\n"+
		"        '%s',\n"+
		"        '9999-12-31 00:00:00-00',\n"+
		"        '%s'", timeNow, c.Origin)
	for _, col = range c.Column {
		//fmt.Fprintf(&b, ",\n        %s", command.SQLEncodeData(col.Data, col.DType))
		fmt.Fprintf(&b, ",\n        %s", col.EncodedData)
	}
	fmt.Fprintf(&b, "\n    );")
	q = b.String()
	if _, err = tx.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	return nil
}

func execDeleteData(c *command.Command, tx *sql.Tx) error {
	t := sqlx.Table{Schema: c.SchemaName, Table: c.TableName}
	// current table
	// subselect a single record
	var b strings.Builder
	b.WriteString("(SELECT __id FROM " + t.SQL() + " WHERE")
	addColumnsToSelect(&b, c.Column)
	b.WriteString(" LIMIT 1)")
	// delete the record
	_, err := tx.ExecContext(context.TODO(), "DELETE FROM "+t.SQL()+" WHERE __id="+b.String())
	if err != nil {
		return err
	}
	// history table
	// subselect matching current record in history table
	b.Reset()
	b.WriteString("(SELECT __id FROM " + t.History().SQL() + " WHERE")
	addColumnsToSelect(&b, c.Column)
	b.WriteString(" AND __current LIMIT 1)")
	// mark as not current
	_, err = tx.ExecContext(context.TODO(),
		"UPDATE "+t.History().SQL()+" SET __current=FALSE, __end=$1 WHERE __id="+b.String(), time.Now().Format(time.RFC3339))
	if err != nil {
		return err
	}
	return nil
}

func addColumnsToSelect(b *strings.Builder, column []command.CommandColumn) {
	for i, c := range column {
		if i != 0 {
			b.WriteString(" AND")
		}
		if c.DType == command.JSONType {
			b.WriteString(" " + c.Name + "::text=" + c.EncodedData + "::text")
		} else {
			b.WriteString(" " + c.Name + "=" + c.EncodedData)
		}
	}
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
