package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/database"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sysdb"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func execCommandList(cl *command.CommandList, db *sql.DB) error {
	var err error
	var c command.Command
	for _, c = range cl.Cmd {
		if err = execCommandSchema(&c, db); err != nil {
			return err
		}
		if err = execCommandData(&c, db); err != nil {
			return fmt.Errorf("%s\n%v", err, c)
		}
	}
	return nil
}

func execCommandSchema(c *command.Command, db *sql.DB) error {
	var err error
	var delta *deltaSchema
	if delta, err = findDeltaSchema(c); err != nil {
		return err
	}
	// TODO can we skip adding the table if we confirm it in sysdb?
	if err = sysdb.AddTable(c.SchemaName, c.TableName, db); err != nil {
		return err
	}
	if err = execDeltaSchema(delta, c.SchemaName, c.TableName, db); err != nil {
		return err
	}
	return nil
}

func execDeltaSchema(delta *deltaSchema, tschema string, tableName string, db *sql.DB) error {
	var err error
	var col deltaColumnSchema
	//if len(delta.column) == 0 {
	//        log.Trace("table %s: no schema changes", util.JoinSchemaTable(tschema, tableName))
	//}
	for _, col = range delta.column {
		// Is this a new column (as opposed to a modification)?
		if col.newColumn {
			log.Trace("table %s: new column: %s %s", util.JoinSchemaTable(tschema, tableName), col.name, command.DataTypeToSQL(col.newType, col.newTypeSize))
			if err = sysdb.AddColumn(tschema, tableName, col.name, col.newType, col.newTypeSize, db); err != nil {
				return err
			}
			continue
		}
		// If the types are the same and they are varchar, both PostgreSQL and
		// Redshift can alter the column in place
		if col.oldType == col.newType && col.oldType == command.VarcharType {
			log.Trace("table %s: alter column: %s %s", util.JoinSchemaTable(tschema, tableName), col.name, command.DataTypeToSQL(col.newType, col.newTypeSize))
			if err = sysdb.AlterColumnVarcharSize(tschema, tableName, col.name, col.newType, col.newTypeSize, db); err != nil {
				return err
			}
			continue
		}
		// Otherwise we have a completely new type
		log.Trace("table %s: rename column %s", util.JoinSchemaTable(tschema, tableName), col.name)
		if err = sysdb.RenameColumnOldType(tschema, tableName, col.name, col.newType, col.newTypeSize, db); err != nil {
			return err
		}
		log.Trace("table %s: new column %s %s", util.JoinSchemaTable(tschema, tableName), col.name, command.DataTypeToSQL(col.newType, col.newTypeSize))
		if err = sysdb.AddColumn(tschema, tableName, col.name, col.newType, col.newTypeSize, db); err != nil {
			return err
		}
	}
	return nil
}

func execCommandData(c *command.Command, db *sql.DB) error {
	var err error
	var tx *sql.Tx
	if tx, err = database.MakeTx(db); err != nil {
		return err
	}
	defer tx.Rollback()
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
		"        __current,\n"+
		"        __start,\n"+
		"        __end", util.JoinSchemaTable(c.SchemaName, c.TableName))
	var col command.CommandColumn
	for _, col = range c.Column {
		fmt.Fprintf(&b, ",\n        \"%s\"", col.Name)
	}
	fmt.Fprintf(&b, "\n"+
		"    ) VALUES (\n"+
		"        TRUE,\n"+
		"        '%s',\n"+
		"        '9999-12-31 00:00:00-00'", timeNow)
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
		"        __end", util.JoinSchemaTable(c.SchemaName, c.TableName+"__"))
	for _, col = range c.Column {
		fmt.Fprintf(&b, ",\n        \"%s\"", col.Name)
	}
	fmt.Fprintf(&b, "\n"+
		"    ) VALUES (\n"+
		"        TRUE,\n"+
		"        '%s',\n"+
		"        '9999-12-31 00:00:00-00'", timeNow)
	for _, col = range c.Column {
		//fmt.Fprintf(&b, ",\n        %s", command.SQLEncodeData(col.Data, col.DType))
		fmt.Fprintf(&b, ",\n        %s", col.EncodedData)
	}
	fmt.Fprintf(&b, "\n    );")
	q = b.String()
	if _, err = tx.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("%s:\n%s", err, q)
	}
	// Commit
	if err = tx.Commit(); err != nil {
		return err
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
		"    WHERE ", util.JoinSchemaTable(c.SchemaName, c.TableName+h))
	var x int
	var col command.CommandColumn
	for x, col = range pkey {
		if x != 0 {
			fmt.Fprintf(&b, " AND\n        ")
		}
		fmt.Fprintf(&b, "%s = %s", col.Name, command.SQLEncodeData(col.Data, col.DType, col.SemanticType))
	}
	fmt.Fprintf(&b, " AND\n        __current = TRUE;")
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
