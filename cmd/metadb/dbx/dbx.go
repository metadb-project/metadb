package dbx

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Table struct {
	S string
	T string
}

func (t *Table) String() string {
	return t.S + "." + t.T
}

func (t *Table) SQL() string {
	return "\"" + t.S + "\".\"" + t.T + "\""
}

func (t *Table) MainSQL() string {
	return "\"" + t.S + "\".\"" + t.T + "__\""
}

type DB struct {
	Host          string
	Port          string
	User          string
	Password      string
	SuperUser     string
	SuperPassword string
	DBName        string
	SSLMode       string
}

//func NewDB(databaseURI string) (*DB, error) {
//	uri, err := url.Parse(databaseURI)
//	if err != nil {
//		return nil, err
//	}
//	db := new(DB)
//	db.Host = uri.Hostname()
//	db.Port = uri.Port()
//	user := uri.User
//	if user == nil {
//		return nil, fmt.Errorf("username/password not specified in database URI: " + databaseURI)
//	}
//	db.User = user.Username()
//	db.Password, _ = user.Password()
//	db.DBName = strings.TrimPrefix(uri.EscapedPath(), "/")
//	db.SSLMode = strings.Join(uri.Query()["sslmode"], ",")
//	return db, nil
//}

func (d *DB) String() string {
	e := *d
	e.Password = ""
	e.SuperPassword = ""
	return fmt.Sprintf("%v", e)
}

func (d *DB) Connect() (*pgx.Conn, error) {
	return d.connectUser(d.User, d.Password)
}

func (d *DB) ConnectSuper() (*pgx.Conn, error) {
	return d.connectUser(d.SuperUser, d.SuperPassword)
}

func (d *DB) connectUser(user, password string) (*pgx.Conn, error) {
	dsn := "connect_timeout=30 host=" + d.Host + " port=" + d.Port + " user=" + user + " password=" + password + " dbname=" + d.DBName + " sslmode=" + d.SSLMode
	dc, err := pgx.Connect(context.TODO(), dsn)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %v: %v", d, err)
	}
	return dc, nil
}

func Close(dc *pgx.Conn) {
	_ = dc.Close(context.TODO())
}

func Rollback(tx pgx.Tx) {
	_ = tx.Rollback(context.TODO())
}

/*
func CreateUser(db *DB, user, password string) error {
	var dbc *pgx.Conn
	var err error
	if dbc, err = Connect(db); err != nil {
		return fmt.Errorf("creating user: %s: %v", user, err)
	}
	defer Close(dbc)

	_, err = dbc.Exec(context.TODO(), "CREATE USER "+user+" PASSWORD '"+password+"'")
	if err != nil {
		return fmt.Errorf("creating user: %s: %v", user, err)
	}
	return nil
}
*/
