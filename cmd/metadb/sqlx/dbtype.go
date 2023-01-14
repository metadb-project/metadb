package sqlx

//func NewDBType(typeName string) (DBType, error) {
//	switch typeName {
//	case "postgresql":
//		return &Postgres{}, nil
//	case "redshift":
//		return &Redshift{}, nil
//	default:
//		return nil, fmt.Errorf("unknown database type: %s", typeName)
//	}
//}
//

//type DBType interface {
//	String() string
//	EncodeString(string) string
//	Id(string) string
//	Identity() string
//	SupportsIndexes() bool
//	CreateIndex(string, *T, []string) string
//	JSONType() string
//}

//func encodeStringPostgres(s string, e bool) string {
//	var b strings.Builder
//	if e {
//		b.WriteString("E'")
//	} else {
//		b.WriteRune('\'')
//	}
//	for _, c := range s {
//		switch c {
//		case '\\':
//			b.WriteString("\\\\")
//		case '\'':
//			b.WriteString("''")
//		case '\b':
//			b.WriteString("\\b")
//		case '\f':
//			b.WriteString("\\f")
//		case '\n':
//			b.WriteString("\\n")
//		case '\r':
//			b.WriteString("\\r")
//		case '\t':
//			b.WriteString("\\t")
//		default:
//			b.WriteRune(c)
//		}
//	}
//	b.WriteRune('\'')
//	return b.String()
//}
