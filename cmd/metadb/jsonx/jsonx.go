package jsonx

import (
	"container/list"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func RewriteJSON(cmdlist *list.List, cmde *list.Element, column *command.CommandColumn) error {
	cmd := cmde.Value.(*command.Command)
	// folio module filter
	if cmd.SchemaName == "folio_source_record" &&
		(cmd.TableName == "marc_records_lb" || cmd.TableName == "edifact_records_lb") {
		return nil
	}

	if column.Data == nil {
		return nil
	}
	// TODO - JSON data is not necessarily a JSON object; it could be any valid JSON data type
	// Unmarshal into interface{} and send that to a function rewriteData()
	// which tests for each kind of JSON-derived type.
	var j interface{}
	if err := json.Unmarshal([]byte(column.Data.(string)), &j); err != nil {
		return fmt.Errorf("rewrite json: %s", err)
	}
	obj, ok := j.(map[string]interface{})
	if !ok {
		return nil
	}
	if err := rewriteObject(cmdlist, cmde, 1, obj, cmd.TableName+"__t"); err != nil {
		return fmt.Errorf("rewrite json: %s", err)
	}
	return nil
}

func rewriteObject(cmdlist *list.List, cmde *list.Element, level int, obj map[string]interface{}, table string) error {
	cmd := cmde.Value.(*command.Command)
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
		if value == nil {
			// For nil values, do not add the column to this record.
			continue
		}
		switch v := value.(type) {
		case []interface{}:
			//if err := rewriteArray(cl, cmd, column, db, level+1, n, v, table+"_"+n); err != nil {
			//        return err
			//}
		case bool:
			sqldata, err := command.DataToSQLData(v, command.BooleanType, "")
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
			s := strconv.FormatFloat(v, 'E', -1, 64)
			sqldata, err := command.DataToSQLData(s, command.NumericType, "")
			if err != nil {
				return err
			}
			cols = append(cols, command.CommandColumn{
				Name:       n,
				DType:      command.NumericType,
				DTypeSize:  0,
				Data:       v,
				SQLData:    sqldata,
				PrimaryKey: 0,
			})
		case map[string]interface{}:
			//if err := rewriteObject(cl, cmd, column, db, level+1, v, table+"_"+n); err != nil {
			//        return err
			//}
		case string:
			sqldata, err := command.DataToSQLData(v, command.TextType, "")
			if err != nil {
				return err
			}
			dtype := command.InferTypeFromString(v)
			cols = append(cols, command.CommandColumn{
				Name:       n,
				DType:      dtype,
				DTypeSize:  0,
				Data:       v,
				SQLData:    sqldata,
				PrimaryKey: 0,
			})
		}

	}
	newcmd := &command.Command{
		Op:              command.MergeOp,
		SchemaName:      cmd.SchemaName,
		TableName:       table,
		Transformed:     true,
		ParentTable:     dbx.Table{Schema: cmd.SchemaName, Table: cmd.TableName},
		Origin:          cmd.Origin,
		Column:          cols,
		SourceTimestamp: cmd.SourceTimestamp,
	}
	_ = cmdlist.InsertAfter(newcmd, cmde)
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
				DType:        command.TextType,
				DTypeSize:    int64(len(v)),
				SemanticType: "",
				Data:         v,
				EncodedData:  command.SQLEncodeData(v, command.TextType, ""),
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
