package jsonx

import (
	"database/sql"
	"encoding/json"

	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func RewriteJSON(cl *command.CommandList, cmd *command.Command, column *command.CommandColumn, db *sql.DB) error {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(column.Data.(string)), &obj); err != nil {
		return err
	}
	if err := rewriteObject(cl, cmd, column, db, 1, obj, cmd.TableName+"_j"); err != nil {
		return err
	}
	return nil
}

func rewriteObject(cl *command.CommandList, cmd *command.Command, column *command.CommandColumn, db *sql.DB, level int, obj map[string]interface{}, table string) error {
	if level > 2 {
		return nil
	}
	var cols []command.CommandColumn
	pkcols := command.PrimaryKeyColumns(cmd.Column)
	pkcolsmap := make(map[string]bool)
	for _, col := range pkcols {
		cols = append(cols, col)
		pkcolsmap[col.Name] = true
	}
	for name, value := range obj {
		n, err := util.DecodeCamelCase(name)
		if err != nil {
			return err
		}
		if pkcolsmap[name] {
			n = "j_" + n
		}
		//log.P("TYPE: %T, VALUE: %v", value, value)
		if value == nil {
			// TODO store null value
			continue
		}
		switch v := value.(type) {
		case []interface{}:
			if err := rewriteArray(cl, cmd, column, db, level+1, n, v, table+"_"+n); err != nil {
				return err
			}
		case bool:
			cols = append(cols, command.CommandColumn{
				Name:         n,
				DType:        command.BooleanType,
				DTypeSize:    0,
				SemanticType: "",
				Data:         v,
				EncodedData:  command.SQLEncodeData(v, command.BooleanType, ""),
				PrimaryKey:   0,
			})
		case float64:
			cols = append(cols, command.CommandColumn{
				Name:         n,
				DType:        command.FloatType,
				DTypeSize:    8,
				SemanticType: "",
				Data:         v,
				EncodedData:  command.SQLEncodeData(v, command.FloatType, ""),
				PrimaryKey:   0,
			})
		case map[string]interface{}:
			if err := rewriteObject(cl, cmd, column, db, level+1, v, table+"_"+n); err != nil {
				return err
			}
		case string:
			cols = append(cols, command.CommandColumn{
				Name:         n,
				DType:        command.VarcharType,
				DTypeSize:    int64(len(v)),
				SemanticType: "",
				Data:         v,
				EncodedData:  command.SQLEncodeData(v, command.VarcharType, ""),
				PrimaryKey:   0,
			})
		default:
		}

	}
	ncmd := command.Command{
		Op:              command.MergeOp,
		SchemaName:      cmd.SchemaName,
		TableName:       table,
		ParentTable:     sqlx.Table{cmd.SchemaName, cmd.TableName},
		Origin:          cmd.Origin,
		Column:          cols,
		ChangeEvent:     cmd.ChangeEvent,
		SourceTimestamp: cmd.SourceTimestamp,
	}
	cl.Cmd = append(cl.Cmd, ncmd)

	return nil
}

func rewriteArray(cl *command.CommandList, cmd *command.Command, column *command.CommandColumn, db *sql.DB, level int, aname string, adata []interface{}, table string) error {
	if level > 2 {
		return nil
	}
	pkcols := command.PrimaryKeyColumns(cmd.Column)
	for i, value := range adata {
		if value == nil {
			// TODO store null value
			continue
		}
		_ = i
		switch v := value.(type) {
		case map[string]interface{}:
			// TODO
		case string:
			var cols []command.CommandColumn
			for _, col := range pkcols {
				cols = append(cols, col)
			}
			cols = append(cols, command.CommandColumn{
				Name:         "ord",
				DType:        command.IntegerType,
				DTypeSize:    4,
				SemanticType: "",
				Data:         i + 1,
				EncodedData:  command.SQLEncodeData(i+1, command.IntegerType, ""),
				PrimaryKey:   len(pkcols) + 1,
			})
			cols = append(cols, command.CommandColumn{
				Name:         aname,
				DType:        command.VarcharType,
				DTypeSize:    int64(len(v)),
				SemanticType: "",
				Data:         v,
				EncodedData:  command.SQLEncodeData(v, command.VarcharType, ""),
				PrimaryKey:   0,
			})
			ncmd := command.Command{
				Op:              command.MergeOp,
				SchemaName:      cmd.SchemaName,
				TableName:       table,
				ParentTable:     sqlx.Table{cmd.SchemaName, cmd.TableName},
				Origin:          cmd.Origin,
				Column:          cols,
				ChangeEvent:     cmd.ChangeEvent,
				SourceTimestamp: cmd.SourceTimestamp,
			}
			cl.Cmd = append(cl.Cmd, ncmd)
		}

	}
	return nil
}
