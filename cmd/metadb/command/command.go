package command

import (
	"container/list"
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/change"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
	"github.com/metadb-project/metadb/cmd/metadb/types"
	"github.com/metadb-project/metadb/cmd/metadb/util"
	"github.com/shopspring/decimal"
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

// CommandGraph is a list of commands in which each command may itself contain a
// list of sub-commands.  A command that is not a sub-command, in other words not
// reachable from another command, is called a "root command."  Root commands
// represent basic change events; sub-commands generally represent transformed
// records.
type CommandGraph struct {
	Commands *list.List
}

func NewCommandGraph() *CommandGraph {
	return &CommandGraph{Commands: list.New()}
}

type Command struct {
	Op              Operation
	SchemaName      string
	TableName       string
	Transformed     bool
	ParentTable     dbx.Table
	Origin          string
	Column          []CommandColumn
	SourceTimestamp string
	Subcommands     *list.List
}

func (c *Command) AddChild(child *Command) {
	if c.Subcommands == nil {
		c.Subcommands = list.New()
	}
	_ = c.Subcommands.PushBack(child)
}

func (c *Command) String() string {
	return fmt.Sprintf("command = %s %s.%s (%v)\n", c.Op, c.SchemaName, c.TableName, c.Column)
}

type CommandColumn struct {
	Name        string
	DType       types.DataType
	DTypeSize   int64
	Data        interface{}
	SQLData     *string
	PrimaryKey  int
	Unavailable bool
}

/*func (c CommandColumn) String() string {
	return fmt.Sprintf("%s=%v", c.Name, c.Data)
}
*/

/*func ColumnsString(cols []CommandColumn) string {
	var b strings.Builder
	for i, c := range cols {
		if i != 0 {
			b.WriteRune(' ')
		}
		b.WriteString(c.String())
	}
	return b.String()
}
*/

func convertTypeSize(coltype string, datatype types.DataType) (int64, error) {
	switch datatype {
	case types.IntegerType:
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
			return 0, fmt.Errorf("internal error: unexpected integer type %q", coltype)
		}
	case types.TextType:
		return 0, nil
	case types.FloatType:
		switch coltype {
		case "float", "float32":
			return 4, nil
		case "double", "float64":
			return 8, nil
		default:
			return 0, fmt.Errorf("internal error: unexpected float type %q", coltype)
		}
	case types.NumericType:
		return 0, nil
	case types.BooleanType:
		return 0, nil
	case types.DateType:
		return 0, nil
	case types.TimeType:
		return 0, nil
	case types.TimetzType:
		return 0, nil
	case types.TimestampType:
		return 0, nil
	case types.TimestamptzType:
		return 0, nil
	case types.UUIDType:
		return 0, nil
	case types.JSONType:
		return 0, nil
	default:
		return 0, fmt.Errorf("convert type size: unknown data type: %s", datatype)
	}
}

func extractPrimaryKey(dedup *log.MessageSet, ce *change.Event) (map[string]int, error) {
	var ok bool
	if ce.Key == nil {
		primaryKeyNotDefined(dedup, ce.Topic)
		return nil, nil
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

func extractColumns(dedup *log.MessageSet, ce *change.Event) ([]CommandColumn, error) {
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
	if primaryKey, err = extractPrimaryKey(dedup, ce); err != nil {
		return nil, err
	}
	if primaryKey == nil {
		return nil, nil
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
		col.Data = fieldData[field]
		if (col.DType == types.TextType || col.DType == types.JSONType) && col.Data != nil {
			// Large values (typically above 8 kB) in PostgreSQL that have been stored using
			// the "TOAST" method are not included in an UPDATE change event where those
			// values were not modified.
			if col.Data.(string) == "__debezium_unavailable_value" {
				col.Data = nil
				col.Unavailable = true
			}
		}
		if col.DType == types.NumericType && col.Data != nil {
			if col.Data, err = decodeNumericBytes(m, col.Data, semtype); err != nil {
				return nil, fmt.Errorf("decoding numeric bytes: %w", err)
			}
		}
		if col.SQLData, err = DataToSQLData(col.Data, col.DType, semtype); err != nil {
			return nil, fmt.Errorf("value: $.payload.after: \"%s\": unknown type: %v", field, err)
		}
		if col.DTypeSize, err = convertTypeSize(ftype, col.DType); err != nil {
			return nil, fmt.Errorf("value: $.payload.after: \"%s\": unknown type size: %v", field, err)
		}
		col.PrimaryKey = primaryKey[field]
		column = append(column, col)
	}
	return column, nil
}

func decodeNumericBytes(fieldMap map[string]any, data any, semtype string) (string, error) {
	if data == nil {
		return "", fmt.Errorf("decoding nil value")
	}
	var err error
	var ok bool
	// Read scale and value bytes.
	var scale int32
	var valuestr string
	switch semtype {
	case "org.apache.kafka.connect.data.Decimal":
		// Read scale from parameters object.
		if scale, err = parameterScale(fieldMap); err != nil {
			return "", fmt.Errorf("reading numeric scale from parameters: %w", err)
		}
		if valuestr, ok = data.(string); !ok {
			return "", fmt.Errorf("data \"%v\" has type %T", data, data)
		}
	case "io.debezium.data.VariableScaleDecimal":
		// Read scale from struct.
		if scale, valuestr, err = structScale(data); err != nil {
			return "", fmt.Errorf("reading numeric scale from struct: %w", err)
		}
	default:
		return "", fmt.Errorf("unsupported numeric type: %s: %v", semtype, fieldMap)
	}
	// Decode bytes.
	var bytes []byte
	if bytes, err = base64.StdEncoding.DecodeString(valuestr); err != nil {
		return "", fmt.Errorf("unable to decode numeric bytes: %q", valuestr)
	}
	var bigInt = new(big.Int)
	bigInt.SetBytes(bytes)
	// go get github.com/shopspring/decimal
	// func NewFromBigInt(value *big.Int, exp int32) Decimal
	// var scale int32 = 2
	var dec decimal.Decimal = decimal.NewFromBigInt(bigInt, -scale)
	var decs string = dec.StringFixed(scale)
	//log.Trace("decoded numeric bytes: %q ==> %s", valuestr, decs)
	return decs, nil
}

/*
	{
	  "type": "bytes",
	  "optional": false,
	  "name": "org.apache.kafka.connect.data.Decimal",
	  "version": 1,
	  "parameters": {
	    "scale": "2",
	    "connect.decimal.precision": "19"
	  },
	  "field": "value"
	}
*/
func parameterScale(fieldMap map[string]any) (int32, error) {
	var ok bool
	var pi any
	if pi, ok = fieldMap["parameters"]; !ok {
		return 0, fmt.Errorf("parameter object not found: %v", fieldMap)
	}
	var pm map[string]any
	if pm, ok = pi.(map[string]any); !ok {
		return 0, fmt.Errorf("expected object: parameters field: %v", pi)
	}
	var si any
	if si, ok = pm["scale"]; !ok {
		return 0, fmt.Errorf("scale parameter not found: %v", pm)
	}
	var s string
	if s, ok = si.(string); !ok {
		return 0, fmt.Errorf("unexpected data type: scale parameter: %v", si)
	}
	var err error
	var scale int64
	if scale, err = strconv.ParseInt(s, 10, 32); err != nil {
		return 0, fmt.Errorf("parse error: scale parameter: %q", s)
	}
	return int32(scale), nil
}

/*
	{
	  "type": "struct",
	  "fields": [
	    {
	      "type": "int32",
	      "optional": false,
	      "field": "scale"
	    },
	    {
	      "type": "bytes",
	      "optional": false,
	      "field": "value"
	    }
	  ],
	  "optional": true,
	  "name": "io.debezium.data.VariableScaleDecimal",
	  "version": 1,
	  "doc": "Variable scaled decimal",
	  "field": "n"
	},
*/
func structScale(data any) (int32, string, error) {
	var ok bool
	var obj map[string]any
	if obj, ok = data.(map[string]any); !ok {
		return 0, "", fmt.Errorf("expected object in payload after: %v", data)
	}
	var si any
	if si, ok = obj["scale"]; !ok {
		return 0, "", fmt.Errorf("scale not found in payload after: %v", obj)
	}
	var scalef float64
	if scalef, ok = si.(float64); !ok {
		return 0, "", fmt.Errorf("unexpected data type in scale: %T: %v", si, obj)
	}
	var scale = int32(scalef)
	if scalef != float64(scale) {
		return 0, "", fmt.Errorf("scale not int32: %v: %v", si, obj)
	}
	var vi any
	if vi, ok = obj["value"]; !ok {
		return 0, "", fmt.Errorf("value not found in payload after: %v", obj)
	}
	var valuestr string
	if valuestr, ok = vi.(string); !ok {
		return 0, "", fmt.Errorf("unexpected data type in value: %T: %v", vi, obj)
	}
	return scale, valuestr, nil
}

func NewCommand(cat *catalog.Catalog, dedup *log.MessageSet, ce *change.Event, schemaPassFilter, schemaStopFilter,
	tableStopFilter []*regexp.Regexp, trimSchemaPrefix, addSchemaPrefix, mapPublicSchema string) (*Command, bool, error) {
	snapshot := false
	// Note: this function returns nil, nil in some cases.
	if ce == nil {
		return nil, false, fmt.Errorf("missing change event")
	}
	var err error
	var c = new(Command)
	if ce.Value == nil || ce.Value.Payload == nil {
		var name string
		var key interface{}
		if ce.Key != nil {
			name = *ce.Key.Schema.Name
			key = ce.Key.Payload
		}
		log.Trace("possible tombstone event: missing value payload in change event: schema=%q, key=%v", name, key)
		return nil, false, nil
	}
	if ce.Value.Payload.Op == nil {
		return nil, false, fmt.Errorf("missing value payload op")
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
		return nil, false, fmt.Errorf("unknown op value in change event: %q", *ce.Value.Payload.Op)
	}
	if ce.Value.Payload.Source == nil {
		return nil, false, fmt.Errorf("missing value payload source: %v", ce.Value.Payload)
	}
	if ce.Value.Payload.Source.TsMs == nil {
		return nil, false, fmt.Errorf("missing value payload source timestamp: %v", ce.Value.Payload.Source)
	}
	// convert ts_ms to string
	i, f := math.Modf(*ce.Value.Payload.Source.TsMs / 1000)
	c.SourceTimestamp = time.Unix(int64(i), int64(f*1000000000)).UTC().Format("2006-01-02 15:04:05.000000000") + "Z"
	if ce.Value.Payload.Source.Schema != nil {
		schema := *ce.Value.Payload.Source.Schema
		if len(schemaPassFilter) > 0 && !util.MatchRegexps(schemaPassFilter, schema) {
			log.Trace("filter: reject: %s", schema)
			return nil, false, nil
		}
		if len(schemaStopFilter) > 0 && util.MatchRegexps(schemaStopFilter, schema) {
			log.Trace("filter: reject: %s", schema)
			return nil, false, nil
		}
		// Rewrite schema name
		if schema == "public" && mapPublicSchema != "" {
			c.SchemaName = mapPublicSchema
		} else {
			if trimSchemaPrefix != "" {
				schema = strings.TrimPrefix(schema, trimSchemaPrefix)
			}
			schema = strings.TrimPrefix(schema, "mod_")
			schema = strings.TrimSuffix(schema, "_storage")
			schema = strings.Replace(schema, "_mod_", "_", 1)
			var origin string
			origin, schema = cat.ExtractOrigin(schema)
			c.Origin = origin
			schema = addSchemaPrefix + schema
			c.SchemaName = schema
		}
	}
	if ce.Value.Payload.Source.Table != nil {
		table := *ce.Value.Payload.Source.Table
		schemaTable := *ce.Value.Payload.Source.Schema + "." + table
		if len(tableStopFilter) > 0 && util.MatchRegexps(tableStopFilter, schemaTable) {
			log.Trace("filter: reject: %s", table)
			return nil, false, nil
		}
		c.TableName = table
	}
	if *ce.Value.Payload.Source.Snapshot == "true" {
		snapshot = true
	}
	if c.Op == TruncateOp {
		return c, snapshot, nil
	}
	if c.Op == DeleteOp {
		switch {
		case ce.Key == nil:
			primaryKeyNotDefined(dedup, ce.Topic)
			return nil, false, nil
		case ce.Key.Schema == nil:
			return nil, false, fmt.Errorf("delete: missing event key schema: %v", ce.Key)
		case ce.Key.Schema.Fields == nil:
			return nil, false, fmt.Errorf("delete: missing event key schema fields: %v", ce.Key)
		case ce.Key.Payload == nil:
			return nil, false, fmt.Errorf("delete: missing event key payload: %v", ce.Key)
		}
		fields := ce.Key.Schema.Fields
		payload := ce.Key.Payload
		for i, m := range fields {
			attr, ok := m["field"].(string)
			if !ok {
				return nil, false, fmt.Errorf("delete: unexpected type: key schema field: %v", m["field"])
			}
			var semtype string
			if m["name"] != nil {
				semtype, ok = m["name"].(string)
				if !ok {
					return nil, false, fmt.Errorf("delete: unexpected type: key schema name: %v", m["name"])
				}
			}
			dt, ok := m["type"].(string)
			if !ok {
				return nil, false, fmt.Errorf("delete: unexpected type: key schema type: %v", m["type"])
			}
			var dtype types.DataType
			dtype, err = convertDataType(dt, semtype)
			if err != nil {
				return nil, false, fmt.Errorf("delete: unknown key schema type: %v", m["type"])
			}
			data := payload[attr]
			var edata *string
			edata, err = DataToSQLData(data, dtype, semtype)
			if err != nil {
				return nil, false, fmt.Errorf("delete: unknown type: %w", err)
			}
			var typesize int64
			typesize, err = convertTypeSize(dt, dtype)
			if err != nil {
				return nil, false, fmt.Errorf("delete: unknown type size: %v", data)
			}
			c.Column = append(c.Column, CommandColumn{
				Name:       attr,
				DType:      dtype,
				DTypeSize:  typesize,
				Data:       data,
				SQLData:    edata,
				PrimaryKey: i + 1,
			})
		}
		return c, snapshot, nil
	}
	if c.Column, err = extractColumns(dedup, ce); err != nil {
		return nil, false, err
	}
	if c.Column == nil {
		return nil, false, nil
	}
	return c, snapshot, nil
}

func primaryKeyNotDefined(dedup *log.MessageSet, topicPtr *string) {
	topic := ""
	if topicPtr != nil {
		topic = *topicPtr
	}
	msg := fmt.Sprintf("primary key not defined: %s", topic)
	if dedup.Insert(msg) {
		log.Warning("%s", msg)
	}
}

// convertDataType converts a literal type and semantic type (provided by a
// change event) to a DataType.
func convertDataType(coltype, semtype string) (types.DataType, error) {
	switch coltype {
	case "boolean":
		return types.BooleanType, nil
	case "int8", "int16":
		return types.IntegerType, nil
	case "int32":
		if strings.HasSuffix(semtype, ".time.Date") {
			return types.DateType, nil
		}
		if strings.HasSuffix(semtype, ".time.Time") {
			return types.TimeType, nil
		}
		return types.IntegerType, nil
	case "int64":
		if strings.HasSuffix(semtype, ".time.MicroTime") {
			return types.TimeType, nil
		}
		if strings.HasSuffix(semtype, ".time.Timestamp") || strings.HasSuffix(semtype, ".time.MicroTimestamp") {
			return types.TimestampType, nil
		}
		return types.IntegerType, nil
	case "float", "double", "float32", "float64":
		return types.FloatType, nil
	case "string":
		if strings.HasSuffix(semtype, ".data.Uuid") {
			return types.UUIDType, nil
		}
		if strings.HasSuffix(semtype, ".data.Json") {
			return types.JSONType, nil
		}
		if strings.HasSuffix(semtype, ".time.ZonedTime") {
			return types.TimetzType, nil
		}
		if strings.HasSuffix(semtype, ".time.ZonedTimestamp") {
			return types.TimestamptzType, nil
		}
		return types.TextType, nil
	case "bytes":
		// if strings.HasSuffix(semtype, ".data.Bits") {
		// 	return , nil
		// }
		if semtype == "org.apache.kafka.connect.data.Decimal" {
			return types.NumericType, nil
		}
		return 0, fmt.Errorf("convert data type: unhandled type: type=%s, semtype=%s", coltype, semtype)
	case "struct":
		if semtype == "io.debezium.data.VariableScaleDecimal" {
			return types.NumericType, nil
		}
		return 0, fmt.Errorf("convert data type: unhandled type: type=%s, semtype=%s", coltype, semtype)
	default:
		return 0, fmt.Errorf("convert data type: unknown data type: %s", coltype)
	}
}

// DataToSQLData converts data to a string ready for encoding to SQL.
func DataToSQLData(data any, datatype types.DataType, semtype string) (*string, error) {
	if data == nil {
		return nil, nil
	}
	switch datatype {
	case types.BooleanType:
		v, ok := data.(bool)
		if !ok {
			return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
		}
		if v {
			s := "true"
			return &s, nil
		}
		s := "false"
		return &s, nil
	case types.IntegerType:
		v, ok := data.(float64)
		if !ok {
			return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
		}
		i := int64(v)
		s := strconv.FormatInt(i, 10)
		return &s, nil
	case types.FloatType:
		v, ok := data.(float64)
		if !ok {
			return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
		}
		s := fmt.Sprintf("%g", v)
		return &s, nil
	case types.DateType:
		v, ok := data.(float64)
		if !ok {
			return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
		}
		s := time.Unix(int64(v*86400), int64(0)).UTC().Format("2006-01-02") + "T00:00:00Z"
		return &s, nil
	case types.TimeType:
		v, ok := data.(float64)
		if !ok {
			return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
		}
		switch {
		case strings.HasSuffix(semtype, ".time.Time"):
			var i, f float64 = math.Modf(v / 1000)
			var t string = time.Unix(int64(i), int64(f*1000000000)).UTC().Format("15:04:05.000000")
			s := fixupSQLTime(t)
			return &s, nil
		case strings.HasSuffix(semtype, ".time.MicroTime"):
			var i, f float64 = math.Modf(v / 1000000)
			var t string = time.Unix(int64(i), int64(f*1000000000)).UTC().Format("15:04:05.000000")
			s := fixupSQLTime(t)
			return &s, nil
		}
	case types.TimestampType:
		v, ok := data.(float64)
		if !ok {
			return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
		}
		switch {
		case strings.HasSuffix(semtype, ".time.Timestamp"):
			var i, f float64 = math.Modf(v / 1000)
			var t string = time.Unix(int64(i), int64(f*1000000000)).UTC().Format("2006-01-02 15:04:05.000000")
			s := fixupSQLTime(t)
			return &s, nil
		case strings.HasSuffix(semtype, ".time.MicroTimestamp"):
			var i, f float64 = math.Modf(v / 1000000)
			var t string = time.Unix(int64(i), int64(f*1000000000)).UTC().Format("2006-01-02 15:04:05.000000")
			s := fixupSQLTime(t)
			return &s, nil
		}
	case types.TextType, types.NumericType, types.UUIDType, types.JSONType, types.TimetzType, types.TimestamptzType:
		s, ok := data.(string)
		if !ok {
			return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
		}
		return &s, nil
	}
	return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
}

// fixupSQLTime prepares a time or timestamp for subsequent SQL encoding.  Any
// fractional trailing zeros are removed.  "T" is added between the date and
// time of a timestamp.  "Z" is appended to specify UTC.  This function does
// not validate the input string but assumes it is a valid time or timestamp
// without a time zone.
func fixupSQLTime(t string) string {
	return strings.Replace(trimFractionalZeros(t), " ", "T", 1) + "Z"
}

// trimFractionalZeros removes trailing zeros from a string if it ends with a
// fraction.  It is intended for trimming time and timestamp strings that do
// not have a time zone suffix.  This function does not validate the input
// string but assumes it is a valid time or timestamp without a time zone.
func trimFractionalZeros(s string) string {
	// fraction is true if we have reached the fractional part.
	fraction := false
	// mark is the offset of the last seen Unicode code point that should
	// not be trimmed.
	mark := -1
	// Scan the string for the last offset where trimming should begin.
	for i, r := range s {
		if fraction {
			if r != '0' {
				mark = i
			}
		} else {
			// When we find '.', we are in the fractional part and
			// do not mark until we know if the next character is
			// '0'.
			if r == '.' {
				fraction = true
				continue
			}
			mark = i
		}
	}
	return s[:mark+1]
}

func PrimaryKeyColumns(columns []CommandColumn) []CommandColumn {
	var pkey []CommandColumn
	for i := range columns {
		if columns[i].PrimaryKey != 0 {
			pkey = append(pkey, columns[i])
		}
	}
	sort.Slice(pkey, func(i, j int) bool {
		return pkey[i].PrimaryKey < pkey[j].PrimaryKey
	})
	return pkey
}

func InferTypeFromString(data string) types.DataType {
	switch {
	case timestamptzRegexp.MatchString(data):
		return types.TimestamptzType
	case timestampRegexp.MatchString(data):
		return types.TimestampType
	default:
		return types.TextType
	}
}

var timestamptzRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|(\+\d+(\:\d+)?))?$`)
var timestampRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$`)
