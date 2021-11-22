package sqlx

import (
	"database/sql"
	"strings"
)

type Postgres struct {
	//Database *sql.DB
}

func OpenPostgres(dataSourceName string) (*DB, error) {
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{DB: db, Type: &Postgres{}}, nil
}

//func (d *Postgres) DB() *sql.DB {
//	return d.Database
//}

func (d *Postgres) String() string {
	return "postgresql"
}

func (d *Postgres) EncodeString(s string) string {
	return encodeStringPostgres(s, true)
}

func (d *Postgres) CreateIndex(name string, table *Table, columns []string) string {
	var clist strings.Builder
	for i, c := range columns {
		if i != 0 {
			clist.WriteString(",")
		}
		clist.WriteString("\"" + c + "\"")
	}
	return "CREATE INDEX " + name + " ON " + table.SQL() + "(" + clist.String() + ")"
	//_, err := d.ExecContext(context.TODO(), q)
	//if err != nil {
	//	log.Error("unable to create index on " + table.SQL() + " (" + colstr + ")")
	//}
	//return nil
}

func (d *Postgres) JSONType() string {
	return "JSON"
}
