package jsonx

import (
	"container/list"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/config"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

// RewriteJSON transforms a JSON object stored in a specified column within a
// command.
func RewriteJSON(cmde *list.Element, column *command.CommandColumn) error {
	cmd := cmde.Value.(*command.Command)
	if column.Data == nil {
		return nil
	}
	// Convert JSON to Go data types.
	var j any
	if err := json.Unmarshal([]byte(column.Data.(string)), &j); err != nil {
		return fmt.Errorf("parsing json: %s", err)
	}
	// Assume it is a JSON object (as opposed to some other JSON data type);
	// otherwise cancel the transformation.
	obj, ok := j.(map[string]any)
	if !ok {
		return nil
	}
	objectLevel := 1
	arrayLevel := 0
	quasikey := make([]command.CommandColumn, 0)
	if err := rewriteExtendedObject(cmde, objectLevel, arrayLevel, "", obj, cmd.TableName+"__t", quasikey); err != nil {
		return fmt.Errorf("rewrite json: %s", err)
	}
	return nil
}

// rewriteExtendedObject transforms a specified JSON object, and recursively the
// JSON fields it contains.  Here "extended" refers to JSON objects nested
// within the specified object; the scalar fields of these nested objects are
// added to the same record as the parent object.  JSON arrays are transformed
// in a new table.  Primary key columns of the root command are included with
// the prefix "__root__" added to the column names.  Scalar fields from parent
// transformed records are also added to the primary key.
func rewriteExtendedObject(cmde *list.Element, objectLevel, arrayLevel int, attrPrefix string, obj map[string]any, table string, quasikey []command.CommandColumn) error {
	cmd := cmde.Value.(*command.Command)
	cols := make([]command.CommandColumn, 0)
	pkcols := command.PrimaryKeyColumns(cmd.Column)
	pkcolsmap := make(map[string]bool) // Needed only temporarily for older implementation.
	for _, col := range pkcols {
		if config.Experimental {
			col.Name = "__root__" + col.Name
		}
		cols = append(cols, col)
		if !config.Experimental {
			pkcolsmap[col.Name] = true
		}
	}
	if err := rewriteObject(cmde, objectLevel, arrayLevel, attrPrefix, obj, table, &cols, pkcolsmap, quasikey); err != nil {
		return fmt.Errorf("rewrite json object: %s", err)
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
	cmd.AddChild(newcmd)
	return nil
}

func rewriteObject(cmde *list.Element, objectLevel, arrayLevel int, attrPrefix string, obj map[string]any, table string, cols *[]command.CommandColumn, pkcolsmap map[string]bool, quasikey []command.CommandColumn) error {
	if objectLevel > 5 {
		return nil
	}
	qkey := slices.Clone(quasikey)
	for name, value := range obj {
		decodedName, err := util.DecodeCamelCase(name)
		if err != nil {
			return fmt.Errorf("converting from camel case: %s: %v", err, obj)
		}
		if !config.Experimental && pkcolsmap[name] {
			continue
		}
		n := attrPrefix + decodedName
		if value == nil {
			continue // For nil values, do not add the column to this record.
		}
		switch v := value.(type) {
		case string:
			if err := rewriteString(arrayLevel, n, v, cols, &qkey); err != nil {
				return err
			}
		case bool:
			if err := rewriteBool(arrayLevel, n, v, cols, &qkey); err != nil {
				return err
			}
		case float64:
			if err := rewriteFloat64(arrayLevel, n, v, cols, &qkey); err != nil {
				return err
			}
		}
	}
	for name, value := range obj {
		decodedName, err := util.DecodeCamelCase(name)
		if err != nil {
			return fmt.Errorf("converting from camel case: %s: %v", err, obj)
		}
		n := attrPrefix + decodedName
		if value == nil {
			continue // For nil values, do not add the column to this record.
		}
		switch v := value.(type) {
		case map[string]any:
			if config.Experimental {
				if err := rewriteObject(cmde, objectLevel+1, arrayLevel, n+"__", v, table, cols, pkcolsmap, qkey); err != nil {
					return err
				}
			}
		case []any:
			if config.Experimental {
				if err := rewriteArray(cmde, objectLevel, arrayLevel+1, n, v, table+"__"+n, pkcolsmap, qkey); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func rewriteArray(cmde *list.Element, objectLevel, arrayLevel int, aname string, adata []any, table string, pkcolsmap map[string]bool, quasikey []command.CommandColumn) error {
	if arrayLevel > 1 {
		return nil
	}
	cmd := cmde.Value.(*command.Command)
	pkcols := command.PrimaryKeyColumns(cmd.Column)
	var delcols []command.CommandColumn
	for _, col := range pkcols {
		col.Name = "__root__" + col.Name
		delcols = append(delcols, col)
	}
	delcmd := &command.Command{
		Op:              command.DeleteOp,
		SchemaName:      cmd.SchemaName,
		TableName:       table,
		Transformed:     true,
		ParentTable:     dbx.Table{Schema: cmd.SchemaName, Table: cmd.TableName},
		Origin:          cmd.Origin,
		Column:          delcols,
		SourceTimestamp: cmd.SourceTimestamp,
	}
	cmd.AddChild(delcmd)
	qkey := slices.Clone(quasikey)
	for i, value := range adata {
		var cols []command.CommandColumn
		for _, col := range pkcols {
			col.Name = "__root__" + col.Name
			cols = append(cols, col)
		}
		pkindex := len(pkcols)
		for _, col := range quasikey {
			pkindex++
			col.PrimaryKey = pkindex
			cols = append(cols, col)
		}
		ord := strconv.Itoa(i + 1)
		cols = append(cols, command.CommandColumn{
			Name:       "__ord__" + aname,
			DType:      command.IntegerType,
			DTypeSize:  4,
			Data:       i + 1,
			SQLData:    &ord,
			PrimaryKey: pkindex + 1,
		})
		if value == nil {
			// TODO store null value
			continue
		}
		switch v := value.(type) {
		case string:
			if err := rewriteString(arrayLevel, aname, v, &cols, &qkey); err != nil {
				return err
			}
		case map[string]any:
			if err := rewriteObject(cmde, objectLevel+1, arrayLevel, aname+"__", v, table, &cols, pkcolsmap, qkey); err != nil {
				return fmt.Errorf("rewrite json: %s", err)
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
		cmd.AddChild(newcmd)
	}
	return nil
}

func rewriteString(arrayLevel int, name string, data string, cols *[]command.CommandColumn, quasikey *[]command.CommandColumn) error {
	sqldata, err := command.DataToSQLData(data, command.TextType, "")
	if err != nil {
		return err
	}
	dtype := command.InferTypeFromString(data)
	c := command.CommandColumn{
		Name:       name,
		DType:      dtype,
		DTypeSize:  0,
		Data:       data,
		SQLData:    sqldata,
		PrimaryKey: 0,
	}
	*cols = append(*cols, c)
	if arrayLevel > 1 {
		*quasikey = append(*quasikey, c)
	}
	return nil
}

func rewriteBool(arrayLevel int, name string, data bool, cols *[]command.CommandColumn, quasikey *[]command.CommandColumn) error {
	sqldata, err := command.DataToSQLData(data, command.BooleanType, "")
	if err != nil {
		return err
	}
	c := command.CommandColumn{
		Name:       name,
		DType:      command.BooleanType,
		DTypeSize:  0,
		Data:       data,
		SQLData:    sqldata,
		PrimaryKey: 0,
	}
	*cols = append(*cols, c)
	if arrayLevel > 1 {
		*quasikey = append(*quasikey, c)
	}
	return nil
}

func rewriteFloat64(arrayLevel int, name string, data float64, cols *[]command.CommandColumn, quasikey *[]command.CommandColumn) error {
	s := strconv.FormatFloat(data, 'E', -1, 64)
	sqldata, err := command.DataToSQLData(s, command.NumericType, "")
	if err != nil {
		return err
	}
	c := command.CommandColumn{
		Name:       name,
		DType:      command.NumericType,
		DTypeSize:  0,
		Data:       data,
		SQLData:    sqldata,
		PrimaryKey: 0,
	}
	*cols = append(*cols, c)
	if arrayLevel > 1 {
		*quasikey = append(*quasikey, c)
	}
	return nil
}
