package command

import (
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/metadb-project/metadb/cmd/internal/uuid"
	"github.com/metadb-project/metadb/cmd/metadb/change"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
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

type DataType int

const (
	UnknownType     = 0
	BooleanType     = 1
	DateType        = 2
	FloatType       = 3
	IntegerType     = 4
	JSONType        = 5
	NumericType     = 6
	TimeType        = 7
	TimestampType   = 8
	TimestamptzType = 9
	TimetzType      = 10
	UUIDType        = 11
	TextType        = 12
)

func (d DataType) String() string {
	switch d {
	case BooleanType:
		return "BooleanType"
	case DateType:
		return "DateType"
	case FloatType:
		return "FloatType"
	case IntegerType:
		return "IntegerType"
	case JSONType:
		return "JSONType"
	case NumericType:
		return "NumericType"
	case TimeType:
		return "TimeType"
	case TimestampType:
		return "TimestampType"
	case TimestamptzType:
		return "TimestamptzType"
	case TimetzType:
		return "TimetzType"
	case UUIDType:
		return "UUIDType"
	case TextType:
		return "TextType"
	default:
		log.Error("data type to string: unknown data type: %d", d)
		return "(unknown type)"
	}
}

func MakeDataType(dataType string) (DataType, int64) {
	switch strings.ToLower(dataType) {
	case "text", "varchar", "character varying":
		return TextType, 0
	case "smallint":
		return IntegerType, 2
	case "integer":
		return IntegerType, 4
	case "bigint":
		return IntegerType, 8
	case "real":
		return FloatType, 4
	case "double precision":
		return FloatType, 8
	case "numeric":
		return NumericType, 0
	case "boolean":
		return BooleanType, 0
	case "date":
		return DateType, 0
	case "time without time zone", "time":
		return TimeType, 0
	case "time with time zone", "timetz":
		return TimetzType, 0
	case "timestamp without time zone", "timestamp":
		return TimestampType, 0
	case "timestamp with time zone", "timestamptz":
		return TimestamptzType, 0
	case "uuid":
		return UUIDType, 0
	case "jsonb":
		return JSONType, 0
	default:
		log.Error("make data type new: unknown data type: %s", dataType)
		return UnknownType, 0
	}
}

// DataTypeToSQL convert a data type and type size to a database type.
func DataTypeToSQL(dtype DataType, typeSize int64) string {
	switch dtype {
	case TextType:
		return "text"
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
	case NumericType:
		return "numeric"
	case BooleanType:
		return "boolean"
	case DateType:
		return "date"
	case TimeType:
		return "time without time zone"
	case TimetzType:
		return "time with time zone"
	case TimestampType:
		return "timestamp without time zone"
	case TimestamptzType:
		return "timestamp with time zone"
	case UUIDType:
		return "uuid"
	case JSONType:
		return "jsonb"
	default:
		return "(unknown)"
	}
}

type CommandList struct {
	Cmd []*Command
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
}

func (c Command) String() string {
	return fmt.Sprintf("command = %s %s.%s (%v)\n", c.Op, c.SchemaName, c.TableName, c.Column)
}

type CommandColumn struct {
	Name        string
	DType       DataType
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

func convertTypeSize(coltype string, datatype DataType) (int64, error) {
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
			return 0, fmt.Errorf("internal error: unexpected integer type %q", coltype)
		}
	case TextType:
		return 0, nil
	case FloatType:
		switch coltype {
		case "float", "float32":
			return 4, nil
		case "double", "float64":
			return 8, nil
		default:
			return 0, fmt.Errorf("internal error: unexpected float type %q", coltype)
		}
	case NumericType:
		return 0, nil
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
	case UUIDType:
		return 0, nil
	case JSONType:
		return 0, nil
	default:
		return 0, fmt.Errorf("convert type size: unknown data type: %s", datatype)
	}
}

func extractPrimaryKey(pkerr map[string]struct{}, ce *change.Event) (map[string]int, error) {
	var ok bool
	if ce.Key == nil {
		var topic string
		if ce.Topic != nil {
			topic = *(ce.Topic)
		} else {
			topic = "(unknown)"
		}
		_, ok = pkerr[topic]
		if !ok {
			log.Warning("primary key not defined: %s", topic)
			pkerr[topic] = struct{}{}
		}
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

func extractColumns(pkerr map[string]struct{}, ce *change.Event) ([]CommandColumn, error) {
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
	if primaryKey, err = extractPrimaryKey(pkerr, ce); err != nil {
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
		if (col.DType == TextType || col.DType == JSONType) && col.Data != nil {
			// Large values (typically above 8 kB) in PostgreSQL that have been stored using
			// the "TOAST" method are not included in an UPDATE change event where those
			// values were not modified.
			if col.Data.(string) == "__debezium_unavailable_value" {
				col.Data = nil
				col.Unavailable = true
			}
		}
		if col.DType == NumericType && col.Data != nil {
			if col.Data, err = decodeNumericBytes(m, col.Data, semtype); err != nil {
				return nil, fmt.Errorf("decoding numeric bytes: %v", err)
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
			return "", fmt.Errorf("reading numeric scale from parameters: %v", err)
		}
		if valuestr, ok = data.(string); !ok {
			return "", fmt.Errorf("data \"%v\" has type %T", data, data)
		}
	case "io.debezium.data.VariableScaleDecimal":
		// Read scale from struct.
		if scale, valuestr, err = structScale(data); err != nil {
			return "", fmt.Errorf("reading numeric scale from struct: %v", err)
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

// func indentJSON(data string) (string, error) {
// 	var err error
// 	var j map[string]interface{}
// 	if err = json.Unmarshal([]byte(data), &j); err != nil {
// 		return "", err
// 	}
// 	var jb []byte
// 	if jb, err = json.MarshalIndent(j, "", "    "); err != nil {
// 		return "", err
// 	}
// 	return string(jb), nil
// }

// var FolioTenant string
var ReshareTenants []string

func NewCommand(pkerr map[string]struct{}, ce *change.Event, schemaPassFilter, schemaStopFilter,
	tableStopFilter []*regexp.Regexp, trimSchemaPrefix, addSchemaPrefix string) (*Command, bool, error) {
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
		if ce != nil && ce.Key != nil {
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
		if trimSchemaPrefix != "" {
			schema = strings.TrimPrefix(schema, trimSchemaPrefix)
		}
		schema = strings.TrimPrefix(schema, "uchicago_")
		schema = strings.TrimPrefix(schema, "lu_")
		schema = strings.TrimPrefix(schema, "dbz_")
		schema = strings.TrimPrefix(schema, "reports_dev_")
		schema = strings.TrimPrefix(schema, "mod_")
		schema = strings.TrimSuffix(schema, "_storage")
		schema = strings.Replace(schema, "_mod_", "_", 1)
		var origin string
		origin, schema = extractOrigin(ReshareTenants, schema)
		c.Origin = origin
		schema = addSchemaPrefix + schema
		c.SchemaName = schema
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
			return nil, false, fmt.Errorf("delete: missing event key: %v", ce.Key)
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
			var dtype DataType
			dtype, err = convertDataType(dt, semtype)
			if err != nil {
				return nil, false, fmt.Errorf("delete: unknown key schema type: %v", m["type"])
			}
			// var scale int32
			// if dtype == NumericType {
			// 	scale, err = parameterScale(m)
			// 	if err != nil {
			// 		return nil, fmt.Errorf("reading numeric scale: %v", err)
			// 	}
			// }
			data := payload[attr]
			// if dtype == JSONType {
			// 	var d string
			// 	d, err = indentJSON(data.(string))
			// 	if err == nil {
			// 		data = d
			// 	}
			// }
			var edata *string
			edata, err = DataToSQLData(data, dtype, semtype)
			if err != nil {
				return nil, false, fmt.Errorf("delete: unknown type: %v", err)
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
	if c.Column, err = extractColumns(pkerr, ce); err != nil {
		return nil, false, err
	}
	if c.Column == nil {
		return nil, false, nil
	}
	return c, snapshot, nil
}

func extractOrigin(prefixes []string, schema string) (string, string) {
	if prefixes != nil {
		var g string
		for _, g = range prefixes {
			var gu = g + "_"
			if strings.HasPrefix(schema, gu) {
				return g, strings.TrimPrefix(schema, gu)
			}
		}
	}
	return "", schema
}

// convertDataType converts a literal type and semantic type (provided by a
// change event) to a DataType.
func convertDataType(coltype, semtype string) (DataType, error) {
	switch coltype {
	case "boolean":
		return BooleanType, nil
	case "int8", "int16":
		return IntegerType, nil
	case "int32":
		if strings.HasSuffix(semtype, ".time.Date") {
			return DateType, nil
		}
		if strings.HasSuffix(semtype, ".time.Time") {
			return TimeType, nil
		}
		return IntegerType, nil
	case "int64":
		if strings.HasSuffix(semtype, ".time.MicroTime") {
			return TimeType, nil
		}
		if strings.HasSuffix(semtype, ".time.Timestamp") || strings.HasSuffix(semtype, ".time.MicroTimestamp") {
			return TimestampType, nil
		}
		return IntegerType, nil
	case "float", "double", "float32", "float64":
		return FloatType, nil
	case "string":
		if strings.HasSuffix(semtype, ".data.Uuid") {
			return UUIDType, nil
		}
		if strings.HasSuffix(semtype, ".data.Json") {
			return JSONType, nil
		}
		if strings.HasSuffix(semtype, ".time.ZonedTime") {
			return TimetzType, nil
		}
		if strings.HasSuffix(semtype, ".time.ZonedTimestamp") {
			return TimestamptzType, nil
		}
		return TextType, nil
	case "bytes":
		// if strings.HasSuffix(semtype, ".data.Bits") {
		// 	return , nil
		// }
		if semtype == "org.apache.kafka.connect.data.Decimal" {
			return NumericType, nil
		}
		return 0, fmt.Errorf("convert data type: unhandled type: type=%s, semtype=%s", coltype, semtype)
	case "struct":
		if semtype == "io.debezium.data.VariableScaleDecimal" {
			return NumericType, nil
		}
		return 0, fmt.Errorf("convert data type: unhandled type: type=%s, semtype=%s", coltype, semtype)
	default:
		return 0, fmt.Errorf("convert data type: unknown data type: %s", coltype)
	}
}

// DataToSQLData converts data to a string ready for encoding to SQL.
func DataToSQLData(data any, datatype DataType, semtype string) (*string, error) {
	if data == nil {
		return nil, nil
	}
	switch datatype {
	case BooleanType:
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
	case IntegerType:
		v, ok := data.(float64)
		if !ok {
			return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
		}
		i := int64(v)
		s := strconv.FormatInt(i, 10)
		return &s, nil
	case FloatType:
		v, ok := data.(float64)
		if !ok {
			return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
		}
		s := fmt.Sprintf("%g", v)
		return &s, nil
	case DateType:
		v, ok := data.(float64)
		if !ok {
			return nil, fmt.Errorf("%s data \"%v\" has type %T", datatype, data, data)
		}
		s := time.Unix(int64(v*86400), int64(0)).UTC().Format("2006-01-02") + "T00:00:00Z"
		return &s, nil
	case TimeType:
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
	case TimestampType:
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
	case TextType, NumericType, UUIDType, JSONType, TimetzType, TimestamptzType:
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

func InferTypeFromString(data string) DataType {
	switch {
	case uuid.IsUUID(data):
		return UUIDType
	case timestamptzRegexp.MatchString(data):
		return TimestamptzType
	case timestampRegexp.MatchString(data):
		return TimestampType
	default:
		return TextType
	}
}

var timestamptzRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|(\+\d+(\:\d+)?))?$`)
var timestampRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$`)
