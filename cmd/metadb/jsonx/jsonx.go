package jsonx

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/config"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/types"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

// RewriteJSON transforms a JSON object stored in a specified column within a
// command.
func RewriteJSON(cat *catalog.Catalog, cmd *command.Command, column *command.CommandColumn, path config.JSONPath, tmap string) error {
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
	table := cmd.TableName + "__" + tmap
	rootkey := command.PrimaryKeyColumns(cmd.Column)
	for i := range rootkey {
		rootkey[i].Name = "__root__" + rootkey[i].Name
	}
	quasikey := make([]command.CommandColumn, 0)
	deletions := make(map[string]struct{})
	if err := rewriteExtendedObject(cat, cmd, obj, table, rootkey, quasikey, path, deletions); err != nil {
		return fmt.Errorf("rewrite json: %s", err)
	}
	return nil
}

// rewriteExtendedObject transforms a specified JSON object, and recursively the
// fields it contains.  Here "extended" refers to objects nested within the
// specified object; the scalar fields of nested objects are added to the same
// record as the parent.  Arrays are transformed in a new table, and the array
// indices are added in a column named with the prefix "__ord__".  Primary key
// columns of the root command are included with the prefix "__root__" added to
// the column names.
func rewriteExtendedObject(cat *catalog.Catalog, cmd *command.Command, obj map[string]any, table string, rootkey, quasikey []command.CommandColumn, path config.JSONPath, deletions map[string]struct{}) error {
	cols := make([]command.CommandColumn, 0)
	cols = append(cols, rootkey...)
	if err := rewriteObject(cat, cmd, "", obj, table, &cols, rootkey, quasikey, path, deletions); err != nil {
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

func rewriteObject(cat *catalog.Catalog, cmd *command.Command, attrPrefix string, obj map[string]any, table string, cols *[]command.CommandColumn, rootkey, quasikey []command.CommandColumn, path config.JSONPath, deletions map[string]struct{}) error {
	if path.Path[len(path.Path)-1] != "" {
		return nil
	}
	qkey := slices.Clone(quasikey)
	for name, value := range obj {
		if value == nil {
			continue
		}
		decodedName, err := util.DecodeCamelCase(name)
		if err != nil {
			return fmt.Errorf("converting from camel case: %s: %v", err, obj)
		}
		n := attrPrefix + decodedName
		switch v := value.(type) {
		case float64:
			if err := rewriteNumber(n, v, cols); err != nil {
				return err
			}
		case string:
			if err := rewriteString(n, v, cols); err != nil {
				return err
			}
		case bool:
			if err := rewriteBoolean(n, v, cols); err != nil {
				return err
			}
		}
	}
	for name, value := range obj {
		if value == nil {
			continue
		}
		switch v := value.(type) {
		case []any:
			p := path.Append(name)
			t := cat.JSONPathLookup(p)
			if t == "" {
				continue
			}
			if err := rewriteArray(cat, cmd, t, v, cmd.TableName+"__"+t, rootkey, qkey, p, deletions); err != nil {
				return err
			}
		case map[string]any:
			p := path.Append(name)
			t := cat.JSONPathLookup(p)
			if t == "" {
				continue
			}
			if err := rewriteObject(cat, cmd, t+"__", v, table, cols, rootkey, qkey, p, deletions); err != nil {
				return err
			}
		}
	}
	return nil
}

func rewriteArray(cat *catalog.Catalog, cmd *command.Command, aname string, adata []any, table string, rootkey, quasikey []command.CommandColumn, path config.JSONPath, deletions map[string]struct{}) error {
	if path.Path[len(path.Path)-1] != "" {
		return nil
	}
	_, ok := deletions[table]
	if !ok {
		delcmd := &command.Command{
			Op:              command.DeleteOp,
			SchemaName:      cmd.SchemaName,
			TableName:       table,
			Transformed:     true,
			ParentTable:     dbx.Table{Schema: cmd.SchemaName, Table: cmd.TableName},
			Origin:          cmd.Origin,
			Column:          rootkey,
			SourceTimestamp: cmd.SourceTimestamp,
		}
		cmd.AddChild(delcmd)
		deletions[table] = struct{}{}
	}
	for i, value := range adata {
		if value == nil {
			continue
		}
		cols := slices.Clone(rootkey)
		pkindex := len(rootkey)
		for _, col := range quasikey {
			pkindex++
			col.PrimaryKey = pkindex
			cols = append(cols, col)
		}
		ord := strconv.Itoa(i + 1)
		ordcol := command.CommandColumn{
			Name:       "__ord__" + aname,
			DType:      types.IntegerType,
			DTypeSize:  4,
			Data:       i + 1,
			SQLData:    &ord,
			PrimaryKey: pkindex + 1,
		}
		cols = append(cols, ordcol)
		qkey := slices.Clone(quasikey)
		qkey = append(qkey, ordcol)
		switch v := value.(type) {
		case float64:
			if err := rewriteNumber(aname, v, &cols); err != nil {
				return err
			}
		case string:
			if err := rewriteString(aname, v, &cols); err != nil {
				return err
			}
		case bool:
			if err := rewriteBoolean(aname, v, &cols); err != nil {
				return err
			}
		case []any: // Not supported
			continue
		case map[string]any:
			if err := rewriteObject(cat, cmd, "", v, table, &cols, rootkey, qkey, path, deletions); err != nil {
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

func rewriteNumber(name string, data float64, cols *[]command.CommandColumn) error {
	s := strconv.FormatFloat(data, 'E', -1, 64)
	sqldata, err := command.DataToSQLData(s, types.NumericType, "")
	if err != nil {
		return err
	}
	c := command.CommandColumn{
		Name:       name,
		DType:      types.NumericType,
		DTypeSize:  0,
		Data:       data,
		SQLData:    sqldata,
		PrimaryKey: 0,
	}
	*cols = append(*cols, c)
	return nil
}

func rewriteString(name string, data string, cols *[]command.CommandColumn) error {
	sqldata, err := command.DataToSQLData(data, types.TextType, "")
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
	return nil
}

func rewriteBoolean(name string, data bool, cols *[]command.CommandColumn) error {
	sqldata, err := command.DataToSQLData(data, types.BooleanType, "")
	if err != nil {
		return err
	}
	c := command.CommandColumn{
		Name:       name,
		DType:      types.BooleanType,
		DTypeSize:  0,
		Data:       data,
		SQLData:    sqldata,
		PrimaryKey: 0,
	}
	*cols = append(*cols, c)
	return nil
}
