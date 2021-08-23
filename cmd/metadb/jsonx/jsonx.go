package jsonx

import (
	"database/sql"
	"encoding/json"

	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func RewriteJSON(cl *command.CommandList, cmd *command.Command, column *command.CommandColumn, db *sql.DB) error {
	var j map[string]interface{}
	if err := json.Unmarshal([]byte(column.Data.(string)), &j); err != nil {
		return err
	}
	var cols []command.CommandColumn
	pkcols := command.PrimaryKeyColumns(cmd.Column)
	pkcolsmap := make(map[string]bool)
	for _, col := range pkcols {
		cols = append(cols, col)
		pkcolsmap[col.Name] = true
	}
	for name, value := range j {
		n, err := util.DecodeCamelCase(name)
		if err != nil {
			return err
		}
		if pkcolsmap[name] {
			n = "j_" + n
		}
		switch v := value.(type) {
		case int:
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
		TableName:       cmd.TableName + "_j",
		ParentTable:     sqlx.Table{cmd.SchemaName, cmd.TableName},
		Origin:          cmd.Origin,
		Column:          cols,
		ChangeEvent:     cmd.ChangeEvent,
		SourceTimestamp: cmd.SourceTimestamp,
	}
	cl.Cmd = append(cl.Cmd, ncmd)

	return nil
}
