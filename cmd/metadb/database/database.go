package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// Open creates and returns a client connection to a specified
// database.
func Open(host, port, user, password, dbname, sslmode string) (*sql.DB, error) {
	var err error
	var connStr = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
	var db *sql.DB
	if db, err = sql.Open("postgres", connStr); err != nil {
		return nil, err
	}
	return db, nil
}

//func OpenMain(conf *config.System) (*sql.DB, error) {
//        var db *config.Database = &conf.Database
//        return Open(db.Host, db.Port, db.SystemUser, db.SystemPassword, db.DBName, db.SSLMode)
//}

//func TestMainConnection(conf *config.System) error {
//        var err error
//        var db *sql.DB
//        if db, err = OpenMain(conf); err != nil {
//                return fmt.Errorf("connecting to database: open: %w", err)
//        }
//        defer db.Close()
//        // Ping database to test connection
//        if err = db.Ping(); err != nil {
//                return fmt.Errorf("connecting to database: ping: %w", err)
//        }
//        return nil
//}
