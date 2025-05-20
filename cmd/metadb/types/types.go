package types

import (
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/log"
)

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
