package sqlx

/*
import (
	"context"
	"github.com/snowflakedb/gosnowflake"
)

func multiStatementContext(dbtype DBType, count int) (context.Context, error) {
	switch dbtype.(type) {
	case *Snowflake:
		ctx, err := gosnowflake.WithMultiStatement(context.Background(), count)
		if err != nil {
			return nil, err
		}
		return ctx, nil
	default:
		return context.TODO(), nil
	}
}
*/

//type Snowflake struct {
//}

//func OpenSnowflake(dsn *DSN) (*DB, error) {
//	s := dsn.User + ":" + dsn.Password + "@" + dsn.Host + ":" + dsn.Port + "/" + dsn.DBName + "?account=" + dsn.Account
//	db, err := sql.Open("snowflake", s)
//	if err != nil {
//		return nil, err
//	}
//	return &DB{DB: db, Type: &Snowflake{}}, nil
//}
//

//func (d *Snowflake) String() string {
//	return "snowflake"
//}
//
//func (d *Snowflake) EncodeString(s string) string {
//	return encodeStringPostgres(s, false)
//}
//
//func (d *Snowflake) Id(identifier string) string {
//	return identifier
//}
//
//func (d *Snowflake) Identity() string {
//	return "IDENTITY"
//}
//
//func (d *Snowflake) SupportsIndexes() bool {
//	return false
//}
//
//func (d *Snowflake) CreateIndex(name string, table *Table, columns []string) string {
//	_ = name
//	_ = table
//	_ = columns
//	return ""
//}
//
//func (d *Snowflake) JSONType() string {
//	return "VARCHAR"
//}
