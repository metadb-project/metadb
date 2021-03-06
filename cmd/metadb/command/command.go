package command

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/metadb-project/metadb/cmd/metadb/change"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/sqlx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

type Operation int

const (
	MergeOp Operation = iota
	DeleteOp
	TruncateOp
)

func (o Operation) String() string {
	switch o {
	case MergeOp:
		return "merge"
	case DeleteOp:
		return "delete"
	case TruncateOp:
		return "truncate"
	default:
		return "(unknown)"
	}
}

type DataType int

const (
	UnknownType = iota
	VarcharType
	IntegerType
	FloatType
	BooleanType
	DateType
	TimeType
	TimetzType
	TimestampType
	TimestamptzType
	JSONType
)

func (d DataType) String() string {
	switch d {
	case VarcharType:
		return "varchar"
	case IntegerType:
		return "integer"
	case FloatType:
		return "float"
	case BooleanType:
		return "boolean"
	case DateType:
		return "date"
	case TimeType:
		return "time"
	case TimetzType:
		return "timetz"
	case TimestampType:
		return "timestamp"
	case TimestamptzType:
		return "timestamptz"
	case JSONType:
		return "json"
	default:
		log.Error("data type to string: unknown data type: %d", d)
		return "(unknown type)"
	}
}

func MakeDataTypeNew(dataType string, charMaxLen int64) (DataType, int64) {
	switch dataType {
	case "character varying":
		return VarcharType, charMaxLen
	case "smallint":
		return IntegerType, 2
	case "integer":
		return IntegerType, 4
	case "bigint":
		return IntegerType, 8
		/*
			case 4:
				return "real"
			case 8:
				return "double precision"
		*/
	case "real":
		return FloatType, 4
	case "double precision":
		return FloatType, 8
	case "boolean":
		return BooleanType, 0
	case "date":
		return DateType, 0
	case "time without time zone":
		return TimeType, 0
	case "time with time zone":
		return TimetzType, 0
	case "timestamp without time zone":
		return TimestampType, 0
	case "timestamp with time zone":
		return TimestamptzType, 0
	case "json":
		return JSONType, 0
	default:
		log.Error("make data type new: unknown data type: %s", dataType)
		return UnknownType, 0
	}
}

func MakeDataType(dtype string) DataType {
	switch dtype {
	case "varchar":
		return VarcharType
	case "integer":
		return IntegerType
	case "float":
		return FloatType
	case "boolean":
		return BooleanType
	case "date":
		return DateType
	case "time":
		return TimeType
	case "timetz":
		return TimetzType
	case "timestamp":
		return TimestampType
	case "timestamptz":
		return TimestamptzType
	case "json":
		return JSONType
	default:
		log.Error("make data type: unknown data type: %s", dtype)
		return UnknownType
	}
}

func DataTypeToSQLNew(dtype DataType, typeSize int64) (string, int64) {
	switch dtype {
	case VarcharType:
		return "character varying", typeSize
	case IntegerType:
		switch typeSize {
		case 2:
			return "smallint", 0
		case 4:
			return "integer", 0
		case 8:
			return "bigint", 0
		default:
			return "(unknown)", 0
		}
	case FloatType:
		switch typeSize {
		case 4:
			return "real", 0
		case 8:
			return "double precision", 0
		default:
			return "(unknown)", 0
		}
	case BooleanType:
		return "boolean", 0
	case DateType:
		return "date", 0
	case TimeType:
		return "time", 0
	case TimetzType:
		return "time with time zone", 0
	case TimestampType:
		return "timestamp without time zone", 0
	case TimestamptzType:
		return "timestamp with time zone", 0
	case JSONType:
		// Postgres only
		return "json", 0
	default:
		return "(unknown)", 0
	}
}

func DataTypeToSQL(dtype DataType, typeSize int64) string {
	switch dtype {
	case VarcharType:
		ts := typeSize
		if ts < 1 {
			ts = 1
		}
		return fmt.Sprintf("varchar(%d)", ts)
	case IntegerType:
		switch typeSize {
		case 2:
			return "smallint"
		case 4:
			return "integer"
		case 8:
			return "bigint"
		default:
			return "(unknown)"
		}
	case FloatType:
		switch typeSize {
		case 4:
			return "real"
		case 8:
			return "double precision"
		default:
			return "(unknown)"
		}
	case BooleanType:
		return "boolean"
	case DateType:
		return "date"
	case TimeType:
		return "time"
	case TimetzType:
		return "time with time zone"
	case TimestampType:
		return "timestamp"
	case TimestamptzType:
		return "timestamptz"
	case JSONType:
		// Postgres only
		return "json"
	default:
		return "(unknown)"
	}
}

type Command struct {
	Op              Operation
	SchemaName      string
	TableName       string
	ParentTable     sqlx.Table
	Origin          string
	Column          []CommandColumn
	ChangeEvent     *change.Event
	SourceTimestamp string
}

type CommandColumn struct {
	Name         string
	DType        DataType
	DTypeSize    int64
	SemanticType string
	Data         interface{}
	EncodedData  string
	PrimaryKey   int
}

func (c Command) String() string {
	return fmt.Sprintf("command = %s %s (%v)\nevent =\n%v", c.Op, util.JoinSchemaTable(c.SchemaName, c.TableName), c.Column, c.ChangeEvent)
}

type CommandList struct {
	Cmd []Command
}

func convertDataType(coltype, semtype string) (DataType, error) {
	switch coltype {
	case "int8":
		fallthrough
	case "int16":
		fallthrough
	case "int32":
		if strings.HasSuffix(semtype, ".time.Time") {
			return TimeType, nil
		}
		fallthrough
	case "int64":
		if strings.HasSuffix(semtype, ".time.MicroTimestamp") || strings.HasSuffix(semtype, ".time.Timestamp") {
			return TimestampType, nil
		}
		if strings.HasSuffix(semtype, ".time.Date") {
			return DateType, nil
		}
		if strings.HasSuffix(semtype, ".time.MicroTime") {
			return TimeType, nil
		}
		return IntegerType, nil
	case "string":
		if strings.HasSuffix(semtype, ".time.ZonedTimestamp") {
			return TimestamptzType, nil
		}
		if strings.HasSuffix(semtype, ".time.ZonedTime") {
			return TimetzType, nil
		}
		if strings.HasSuffix(semtype, ".data.Json") {
			return JSONType, nil
		}
		return VarcharType, nil
	case "boolean":
		return BooleanType, nil
	default:
		return 0, fmt.Errorf("convert data type: unknown data type: %s", coltype)
	}
}

func convertTypeSize(data interface{}, coltype string, datatype DataType) (int64, error) {
	var ok bool
	// if data == nil {
	// 	return 0, nil
	// }
	switch datatype {
	case IntegerType:
		switch coltype {
		case "int8":
			return 1, nil
		case "int16":
			return 2, nil
		case "int32":
			return 4, nil
		case "int64":
			return 8, nil
		default:
			return 0, fmt.Errorf("internal error: unexpected type %q", coltype)
		}
	case VarcharType:
		if data == nil {
			return 1, nil
		}
		var s string
		if s, ok = data.(string); !ok {
			return 0, fmt.Errorf("internal error: varchar data %q not string type", data)
		}
		return int64(len(s)), nil
	case BooleanType:
		return 0, nil
	case DateType:
		return 0, nil
	case TimeType:
		return 0, nil
	case TimetzType:
		return 0, nil
	case TimestampType:
		return 0, nil
	case TimestamptzType:
		return 0, nil
	case JSONType:
		return 0, nil
	default:
		return 0, fmt.Errorf("convert type size: unknown data type: %s", datatype)
	}
}

func extractPrimaryKey(ce *change.Event) (map[string]int, error) {
	var ok bool
	if ce.Key == nil {
		var topic string
		if ce.Message.TopicPartition.Topic != nil {
			topic = *ce.Message.TopicPartition.Topic
		} else {
			topic = "(unknown)"
		}
		return nil, fmt.Errorf("primary key not defined: %s", topic)
	}
	if ce.Key.Schema == nil || ce.Key.Schema.Fields == nil {
		return nil, fmt.Errorf("key: $.schema.fields not found")
	}
	var primaryKey = make(map[string]int)
	var x int
	var f interface{}
	for x, f = range ce.Key.Schema.Fields {
		var m map[string]interface{}
		if m, ok = f.(map[string]interface{}); !ok {
			return nil, fmt.Errorf("key: $.schema.fields: unexpected type")
		}
		var fi interface{}
		if fi, ok = m["field"]; !ok {
			return nil, fmt.Errorf("key: $.schema.fields: missing field name")
		}
		var field string
		if field, ok = fi.(string); !ok {
			return nil, fmt.Errorf("key: $.schema.fields: field name has unexpected type")
		}
		primaryKey[field] = x + 1
	}
	return primaryKey, nil
}

func extractColumns(ce *change.Event) ([]CommandColumn, error) {
	var err error
	var ok bool
	// Extract field data from payload
	if ce.Value == nil || ce.Value.Payload == nil || ce.Value.Payload.After == nil {
		return nil, fmt.Errorf("value: $.payload.after not found")
	}
	var fieldData map[string]interface{} = ce.Value.Payload.After
	// Extract fields from schema
	if ce.Value == nil || ce.Value.Schema == nil || ce.Value.Schema.Fields == nil {
		return nil, fmt.Errorf("value: $.schema.fields not found")
	}
	var after map[string]interface{}
	var schemaField map[string]interface{}
	for _, schemaField = range ce.Value.Schema.Fields {
		var f interface{}
		if f = schemaField["field"]; f == nil {
			continue
		}
		var fs string
		if fs, ok = f.(string); !ok {
			continue
		}
		if fs == "after" {
			after = schemaField
			break
		}
	}
	if after == nil {
		return nil, fmt.Errorf("value: $.schema.fields: \"after\" not found")
	}
	var af interface{}
	if af = after["fields"]; af == nil {
		return nil, fmt.Errorf("value: $.schema.fields: \"fields\" not found")
	}
	// var afi []map[string]interface{}
	var afi []interface{}
	if afi, ok = af.([]interface{}); !ok {
		return nil, fmt.Errorf("value: $.schema.fields: \"fields\" not expected type")
	}
	var primaryKey map[string]int
	if primaryKey, err = extractPrimaryKey(ce); err != nil {
		return nil, err
	}
	var column []CommandColumn
	var i interface{}
	for _, i = range afi {
		var m map[string]interface{}
		if m, ok = i.(map[string]interface{}); !ok {
			return nil, fmt.Errorf("value: $.schema.fields: \"fields\" not expected type")
		}
		// Field name
		var fi interface{}
		if fi, ok = m["field"]; !ok {
			return nil, fmt.Errorf("value: $.schema.fields: \"field\" not found")
		}
		var field string
		if field, ok = fi.(string); !ok {
			return nil, fmt.Errorf("value: $.schema.fields: \"field\" not expected type")
		}
		// Field type
		var ti interface{}
		if ti, ok = m["type"]; !ok {
			return nil, fmt.Errorf("value: $.schema.fields: \"type\" not found")
		}
		var ftype string
		if ftype, ok = ti.(string); !ok {
			return nil, fmt.Errorf("value: $.schema.fields: \"type\" not expected data type")
		}
		// Field semantic type
		var semtype string
		var si interface{}
		if si, ok = m["name"]; ok {
			if semtype, ok = si.(string); !ok {
				return nil, fmt.Errorf("value: $.schema.fields: \"name\" not expected data type")
			}
		}

		var col CommandColumn
		col.Name = field
		if col.DType, err = convertDataType(ftype, semtype); err != nil {
			return nil, fmt.Errorf("value: $.schema.fields: \"type\": %s", err)
		}
		col.SemanticType = semtype
		col.Data = fieldData[field]
		var data interface{}
		if col.DType == JSONType {
			if data, err = indentJSON(col.Data.(string)); err != nil {
				data = col.Data
			}
		} else {
			data = col.Data
		}
		col.EncodedData = SQLEncodeData(data, col.DType, col.SemanticType)
		if col.DTypeSize, err = convertTypeSize(col.EncodedData, ftype, col.DType); err != nil {
			return nil, fmt.Errorf("value: $.payload.after: \"%s\": unknown type size", field)
		}
		col.PrimaryKey = primaryKey[field]
		column = append(column, col)
	}
	return column, nil
}

func indentJSON(data string) (string, error) {
	var err error
	var j map[string]interface{}
	if err = json.Unmarshal([]byte(data), &j); err != nil {
		return "", err
	}
	var jb []byte
	if jb, err = json.MarshalIndent(j, "", "    "); err != nil {
		return "", err
	}
	return string(jb), nil
}

var Tenants []string

// Returns nil, nil in some cases.
func NewCommand(ce *change.Event, schemaPassFilter []*regexp.Regexp, schemaPrefix string) (*Command, error) {
	if ce == nil {
		return nil, fmt.Errorf("missing change event")
	}
	var err error
	var c = new(Command)
	c.ChangeEvent = ce
	if ce.Value == nil || ce.Value.Payload == nil {
		var name string
		var key interface{}
		if ce != nil && ce.Key != nil {
			name = *ce.Key.Schema.Name
			key = ce.Key.Payload
		}
		log.Trace("possible tombstone event: missing value payload in change event: schema=%q, key=%v", name, key)
		return nil, nil
	}
	if ce.Value.Payload.Op == nil {
		return nil, fmt.Errorf("missing value payload op")
	}
	switch *ce.Value.Payload.Op {
	case "c":
		c.Op = MergeOp
	case "r":
		c.Op = MergeOp
	case "u":
		c.Op = MergeOp
	case "d":
		c.Op = DeleteOp
	case "t":
		c.Op = TruncateOp
	default:
		return nil, fmt.Errorf("unknown op value in change event: %q", *ce.Value.Payload.Op)
	}
	if ce.Value.Payload.Source == nil {
		return nil, fmt.Errorf("missing value payload source: %v", ce.Value.Payload)
	}
	if ce.Value.Payload.Source.TsMs == nil {
		return nil, fmt.Errorf("missing value payload source timestamp: %v", ce.Value.Payload.Source)
	}
	// convert ts_ms to string
	i, f := math.Modf(*ce.Value.Payload.Source.TsMs / 1000)
	c.SourceTimestamp = time.Unix(int64(i), int64(f*1000000000)).UTC().Format("2006-01-02 15:04:05.000000000") + "Z"
	if ce.Value.Payload.Source.Schema != nil {
		if len(schemaPassFilter) > 0 && !util.MatchRegexps(schemaPassFilter, *ce.Value.Payload.Source.Schema) {
			log.Trace("filter: reject: %s", *ce.Value.Payload.Source.Schema)
			return nil, nil
		}
		// TODO the mapping is currently hardcoded
		var schema string = strings.TrimPrefix(*ce.Value.Payload.Source.Schema, "dbz_")
		schema = strings.TrimPrefix(schema, "reports_dev_")
		schema = strings.TrimPrefix(schema, "mod_")
		schema = strings.TrimSuffix(schema, "_storage")
		schema = strings.Replace(schema, "_mod_", "_", 1)
		var origin string
		origin, schema = extractOrigin(Tenants, schema)
		c.Origin = origin
		schema = schemaPrefix + schema
		c.SchemaName = schema
	} else {
		c.SchemaName = ""
	}
	c.TableName = *ce.Value.Payload.Source.Table
	if c.Op == TruncateOp {
		return c, nil
	}
	if c.Op == DeleteOp {
		switch {
		case ce.Key == nil:
			return nil, fmt.Errorf("delete: missing event key: %v", ce.Key)
		case ce.Key.Schema == nil:
			return nil, fmt.Errorf("delete: missing event key schema: %v", ce.Key)
		case ce.Key.Schema.Fields == nil:
			return nil, fmt.Errorf("delete: missing event key schema fields: %v", ce.Key)
		case ce.Key.Payload == nil:
			return nil, fmt.Errorf("delete: missing event key payload: %v", ce.Key)
		}
		fields := ce.Key.Schema.Fields
		payload := ce.Key.Payload
		for i, m := range fields {
			attr, ok := m["field"].(string)
			if !ok {
				return nil, fmt.Errorf("delete: unexpected type: key schema field: %v", m["field"])
			}
			var semtype string
			if m["name"] != nil {
				semtype, ok = m["name"].(string)
				if !ok {
					return nil, fmt.Errorf("delete: unexpected type: key schema name: %v", m["name"])
				}
			}
			dt, ok := m["type"].(string)
			if !ok {
				return nil, fmt.Errorf("delete: unexpected type: key schema type: %v", m["type"])
			}
			dtype, err := convertDataType(dt, semtype)
			if err != nil {
				return nil, fmt.Errorf("delete: unknown key schema type: %v", m["type"])
			}
			data := payload[attr]
			if dtype == JSONType {
				d, err := indentJSON(data.(string))
				if err == nil {
					data = d
				}
			}
			edata := SQLEncodeData(data, dtype, semtype)
			typesize, err := convertTypeSize(edata, dt, dtype)
			if err != nil {
				return nil, fmt.Errorf("delete: unknown type size: %v", data)
			}
			c.Column = append(c.Column, CommandColumn{
				Name:         attr,
				DType:        dtype,
				DTypeSize:    typesize,
				SemanticType: semtype,
				Data:         data,
				EncodedData:  edata,
				PrimaryKey:   i + 1,
			})
		}
		return c, nil
	}
	if c.Column, err = extractColumns(ce); err != nil {
		return nil, err
	}
	return c, nil
}

func extractOrigin(prefixes []string, schema string) (origin, newSchema string) {
	var g string
	for _, g = range prefixes {
		var gu = g + "_"
		if strings.HasPrefix(schema, gu) {
			origin = g
			newSchema = strings.TrimPrefix(schema, gu)
			return
		}
	}
	origin = ""
	newSchema = schema
	return
}

func SQLEncodeData(data interface{}, datatype DataType, semtype string) string {
	if data == nil {
		return "NULL"
	}
	switch v := data.(type) {
	case string:
		if datatype == TimestamptzType || datatype == TimetzType {
			return "'" + v + "'"
		}
		return util.PostgresEncodeString(v, true)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		if datatype == DateType {
			return "'" + time.Unix(int64(v*86400), int64(0)).UTC().Format("2006-01-02") + "'"
		}
		if datatype == TimestampType && strings.HasSuffix(semtype, ".time.MicroTimestamp") {
			var i, f float64 = math.Modf(v / 1000000)
			return "'" + time.Unix(int64(i), int64(f*1000000000)).UTC().Format("2006-01-02 15:04:05.000000000") + "'"
		}
		if datatype == TimestampType && strings.HasSuffix(semtype, ".time.Timestamp") {
			var i, f float64 = math.Modf(v / 1000)
			return "'" + time.Unix(int64(i), int64(f*1000000000)).UTC().Format("2006-01-02 15:04:05.000000000") + "'"
		}
		if datatype == TimeType && strings.HasSuffix(semtype, ".time.MicroTime") {
			var i, f float64 = math.Modf(v / 1000000)
			return "'" + time.Unix(int64(i), int64(f*1000000000)).UTC().Format("15:04:05.000000000") + "'"
		}
		if datatype == TimeType && strings.HasSuffix(semtype, ".time.Time") {
			var i, f float64 = math.Modf(v / 1000)
			return "'" + time.Unix(int64(i), int64(f*1000000000)).UTC().Format("15:04:05.000000000") + "'"
		}
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("(unknown:%T)", data)
	}
}

func PrimaryKeyColumns(columns []CommandColumn) []CommandColumn {
	var pkey []CommandColumn
	for _, col := range columns {
		if col.PrimaryKey != 0 {
			pkey = append(pkey, col)
		}
	}
	sort.Slice(pkey, func(i, j int) bool {
		return pkey[i].PrimaryKey < pkey[j].PrimaryKey
	})
	return pkey
}
