package sqlx

import (
	"database/sql"
)

type Redshift struct {
	//Database *sql.DB
}

func OpenRedshift(dataSourceName string) (*DB, error) {
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{DB: db, Type: &Redshift{}}, nil
}

//func (d *Redshift) DB() *sql.DB {
//	return d.Database
//}

func (d *Redshift) String() string {
	return "redshift"
}

func (d *Redshift) EncodeString(s string) string {
	return encodeStringPostgres(s, false)
}

func (d *Redshift) CreateIndex(name string, table *Table, columns []string) string {
	_ = name
	_ = table
	_ = columns
	return ""
}

func (d *Redshift) JSONType() string {
	return "VARCHAR(65535)"
}
