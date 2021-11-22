package sysdb

import "github.com/metadb-project/metadb/cmd/metadb/sqlx"

func ReadDataSource(spec string) (string, string, error) {
	sysMu.Lock()
	defer sysMu.Unlock()

	var err error
	host, err, _ := getConfig(spec + ".host")
	if err != nil {
		return "", "", err
	}
	port, err, _ := getConfig(spec + ".port")
	if err != nil {
		return "", "", err
	}
	dbname, err, _ := getConfig(spec + ".dbname")
	if err != nil {
		return "", "", err
	}
	user, err, _ := getConfig(spec + ".adminuser")
	if err != nil {
		return "", "", err
	}
	password, err, _ := getConfig(spec + ".adminpassword")
	if err != nil {
		return "", "", err
	}
	sslmode, err, _ := getConfig(spec + ".sslmode")
	if err != nil {
		return "", "", err
	}
	dsn := sqlx.PostgresDSN(host, port, dbname, user, password, sslmode)
	dbtype, err, _ := getConfig(spec + ".type")
	if err != nil {
		return "", "", err
	}
	return dbtype, dsn, nil
}

func ReadDatabaseConnectors() ([]*DatabaseConnector, error) {
	var cmap = make(map[string]map[string]string)
	var err error
	if cmap, err = readConfigMap("db"); err != nil {
		return nil, err
	}
	var dbc []*DatabaseConnector
	var name string
	var conf map[string]string
	for name, conf = range cmap {
		dbc = append(dbc, &DatabaseConnector{
			Name:            name,
			Type:            conf["type"],
			DBHost:          conf["host"],
			DBPort:          conf["port"],
			DBName:          conf["dbname"],
			DBAdminUser:     conf["adminuser"],
			DBAdminPassword: conf["adminpassword"],
			DBSuperUser:     conf["superuser"],
			DBSuperPassword: conf["superpassword"],
			DBUsers:         conf["users"],
			DBSSLMode:       conf["sslmode"],
		})
	}
	return dbc, nil
}
