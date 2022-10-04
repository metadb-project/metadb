package dbx

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v4"
)

type DB struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
}

func NewDB(databaseURI string) (*DB, error) {
	uri, err := url.Parse(databaseURI)
	if err != nil {
		return nil, err
	}
	db := new(DB)
	db.Host = uri.Hostname()
	db.Port = uri.Port()
	user := uri.User
	if user == nil {
		return nil, fmt.Errorf("username/password not specified in database URI: " + databaseURI)
	}
	db.User = user.Username()
	db.Password, _ = user.Password()
	db.DBName = strings.TrimPrefix(uri.EscapedPath(), "/")
	db.SSLMode = strings.Join(uri.Query()["sslmode"], ",")
	return db, nil
}

func (d *DB) String() string {
	var sslmode string
	if d.SSLMode != "" {
		sslmode = " sslmode=" + d.SSLMode
	}
	return "host=" + d.Host + " port=" + d.Port + " user=" + d.User + " password=" + d.Password + " dbname=" + d.DBName + sslmode
}

func Connect(db *DB) (*pgx.Conn, error) {
	var dbc *pgx.Conn
	var err error
	if dbc, err = pgx.Connect(context.TODO(), db.String()); err != nil {
		return nil, fmt.Errorf("connecting to database: %s: %v", db.DBName, err)
	}
	return dbc, nil
}

func Close(dbc *pgx.Conn) {
	_ = dbc.Close(context.TODO())
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
