package database

import (
	"fmt"
	"strings"
)

type System interface {
	JSONType() string
	CurrentTimestamp() string
	EncodeStringConst(string) string
	RedshiftKeys(string, string) string
}

/*func SelectDBMSType(dbms string) (System, error) {
	switch dbms {
	case "postgresql":
		return Postgres{}, nil
	case "redshift":
		return Redshift{}, nil
	default:
		return nil, fmt.Errorf("unknown database type: %s", dbms)
	}
}
*/

type Postgres struct {
}

func (p Postgres) JSONType() string {
	return "JSON"
}

func (p Postgres) CurrentTimestamp() string {
	return "CURRENT_TIMESTAMP"
}

func (p Postgres) EncodeStringConst(s string) string {
	return encodeString(s, true)
}

func (p Postgres) RedshiftKeys( /*distkey string, sortkey string*/ ) string {
	return ""
}

func (p Postgres) String() string {
	return "postgresql"
}

type Redshift struct {
}

func (r Redshift) JSONType() string {
	return "VARCHAR(65535)"
}

func (r Redshift) CurrentTimestamp() string {
	return "SYSDATE"
}

func (r Redshift) EncodeStringConst(s string) string {
	return encodeString(s, false)
}

func (r Redshift) RedshiftKeys(distkey string, sortkey string) string {
	return fmt.Sprintf(" DISTKEY(%s) COMPOUND SORTKEY(%s)",
		distkey, sortkey)
}

func (r Redshift) String() string {
	return "redshift"
}

func encodeString(s string, e bool) string {
	var b strings.Builder
	if e {
		b.WriteString("E'")
	} else {
		b.WriteRune('\'')
	}
	for _, c := range s {
		switch c {
		case '\\':
			b.WriteString("\\\\")
		case '\'':
			b.WriteString("''")
		case '\b':
			b.WriteString("\\b")
		case '\f':
			b.WriteString("\\f")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		default:
			b.WriteRune(c)
		}
	}
	b.WriteRune('\'')
	return b.String()
}
