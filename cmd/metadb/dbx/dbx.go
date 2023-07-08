package dbx

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Queryable interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var _ Queryable = (*pgxpool.Pool)(nil)
var _ Queryable = (*pgx.Conn)(nil)
var _ Queryable = (pgx.Tx)(nil)

type Table struct {
	S string
	T string
}

type Column struct {
	S string
	T string
	C string
}

func (t Table) String() string {
	return t.S + "." + t.T
}

func (t Table) Main() Table {
	return Table{S: t.S, T: t.T + "__"}
}

func (t Table) SQL() string {
	return "\"" + t.S + "\".\"" + t.T + "\""
}

func (t Table) MainSQL() string {
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
	return d.connect(d.User, d.Password)
}

func (d *DB) ConnectSuper() (*pgx.Conn, error) {
	return d.connect(d.SuperUser, d.SuperPassword)
}

func setDatabaseParameters(ctx context.Context, dc *pgx.Conn) error {
	q := "SET idle_in_transaction_session_timeout=0"
	if _, err := dc.Exec(ctx, q); err != nil {
		return err
	}
	q = "SET idle_session_timeout=0"
	_, _ = dc.Exec(ctx, q) // Temporarily allow for PostgreSQL versions < 14
	q = "SET statement_timeout=0"
	if _, err := dc.Exec(ctx, q); err != nil {
		return err
	}
	q = "SET timezone='UTC'"
	if _, err := dc.Exec(ctx, q); err != nil {
		return err
	}
	return nil
}

func (d *DB) connect(user, password string) (*pgx.Conn, error) {
	dc, err := pgx.Connect(context.TODO(), d.ConnString(user, password))
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %s: %v", user, err)
	}
	err = setDatabaseParameters(context.TODO(), dc)
	if err != nil {
		return nil, err
	}
	return dc, nil
}

func (d DB) ConnString(user, password string) string {
	return "connect_timeout=30 host=" + d.Host + " port=" + d.Port + " user=" + user + " password=" + password +
		" dbname=" + d.DBName + " sslmode=" + d.SSLMode
}

func Close(dc *pgx.Conn) {
	_ = dc.Close(context.TODO())
}

func Rollback(tx pgx.Tx) {
	_ = tx.Rollback(context.TODO())
}

func VacuumAnalyze(dc *pgx.Conn, table Table) error {
	if err := Vacuum(dc, table); err != nil {
		return err
	}
	if err := Analyze(dc, table); err != nil {
		return err
	}
	return nil
}

func Vacuum(dc *pgx.Conn, table Table) error {
	q := "VACUUM (PARALLEL 0) " + table.SQL()
	if _, err := dc.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("vacuuming: %s: %v", table, err)
	}
	return nil
}

func Analyze(dc *pgx.Conn, table Table) error {
	q := "ANALYZE " + table.SQL()
	if _, err := dc.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("analyzing: %s: %v", table, err)
	}
	return nil
}

func NewPool(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	config.AfterConnect = setDatabaseParameters
	config.MaxConns = 10
	dp, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return dp, nil
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
