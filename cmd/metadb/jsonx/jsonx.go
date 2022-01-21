package jsonx

import (
	"encoding/json"
	"fmt"

	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func RewriteJSON(cl *command.CommandList, cmd *command.Command, column *command.CommandColumn) error {
	if column.Data == nil {
		return nil
	}
	// TODO - JSON data is not necessarily a JSON object; it could be any valid JSON data type
	// Unmarshal into interface{} and send that to a function rewriteData()
	// which tests for each kind of JSON-derived type.
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(column.Data.(string)), &obj); err != nil {
		return fmt.Errorf("rewrite json: %s", err)
	}
	if err := rewriteObject(cl, cmd, 1, obj, cmd.TableName+"__t"); err != nil {
		return fmt.Errorf("rewrite json: %s", err)
	}
	return nil
}

func rewriteObject(cl *command.CommandList, cmd *command.Command, level int, obj map[string]interface{}, table string) error {
	if level > 2 {
		return nil
	}
	cols := make([]command.CommandColumn, 0)
	pkcols := command.PrimaryKeyColumns(cmd.Column)
	pkcolsmap := make(map[string]bool)
	for _, col := range pkcols {
		cols = append(cols, col)
		pkcolsmap[col.Name] = true
	}
	for name, value := range obj {
		n, err := util.DecodeCamelCase(name)
		if err != nil {
			return fmt.Errorf("converting from camel case: %s: %v", err, obj)
		}
		if pkcolsmap[name] {
			//n = "j_" + n
			continue
		}
		//log.P("TYPE: %T, VALUE: %v", value, value)
		if value == nil {
			// TODO store null value
			continue
		}
		switch v := value.(type) {
		case []interface{}:
			//if err := rewriteArray(cl, cmd, column, db, level+1, n, v, table+"_"+n); err != nil {
			//        return err
			//}
		case bool:
			sqldata, err := command.ToSQLData(v, command.BooleanType, "")
			if err != nil {
				return err
			}
			cols = append(cols, command.CommandColumn{
				Name:       n,
				DType:      command.BooleanType,
				DTypeSize:  0,
				Data:       v,
				SQLData:    sqldata,
				PrimaryKey: 0,
			})
		case float64:
			sqldata, err := command.ToSQLData(v, command.FloatType, "")
			if err != nil {
				return err
			}
			cols = append(cols, command.CommandColumn{
				Name:       n,
				DType:      command.FloatType,
				DTypeSize:  8,
				Data:       v,
				SQLData:    sqldata,
				PrimaryKey: 0,
			})
		case map[string]interface{}:
			//if err := rewriteObject(cl, cmd, column, db, level+1, v, table+"_"+n); err != nil {
			//        return err
			//}
		case string:
			sqldata, err := command.ToSQLData(v, command.VarcharType, "")
			if err != nil {
				return err
			}
			cols = append(cols, command.CommandColumn{
				Name:       n,
				DType:      command.VarcharType,
				DTypeSize:  int64(len(v)),
				Data:       v,
				SQLData:    sqldata,
				PrimaryKey: 0,
			})
		default:
		}

	}
	ncmd := command.Command{
		Op:              command.MergeOp,
		SchemaName:      cmd.SchemaName,
		TableName:       table,
		ParentTable:     sqlx.Table{Schema: cmd.SchemaName, Table: cmd.TableName},
		Origin:          cmd.Origin,
		Column:          cols,
		ChangeEvent:     cmd.ChangeEvent,
		SourceTimestamp: cmd.SourceTimestamp,
	}
	cl.Cmd = append(cl.Cmd, ncmd)

	return nil
}

/*func rewriteArray(cl *command.CommandList, cmd *command.Command, column *command.CommandColumn, db *sql.DB, level int, aname string, adata []interface{}, table string) error {
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
*/
