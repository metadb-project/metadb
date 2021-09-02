package sysdb

import "github.com/metadb-project/metadb/cmd/metadb/sqlx"

func ReadDataSourceName(spec string) (*sqlx.DataSourceName, error) {
	sysMu.Lock()
	defer sysMu.Unlock()

	dsn := new(sqlx.DataSourceName)
	var err error
	dsn.Host, err, _ = getConfig(spec + ".host")
	if err != nil {
		return nil, err
	}
	dsn.Port, err, _ = getConfig(spec + ".port")
	if err != nil {
		return nil, err
	}
	dsn.DBName, err, _ = getConfig(spec + ".dbname")
	if err != nil {
		return nil, err
	}
	dsn.User, err, _ = getConfig(spec + ".adminuser")
	if err != nil {
		return nil, err
	}
	dsn.Password, err, _ = getConfig(spec + ".adminpassword")
	if err != nil {
		return nil, err
	}
	dsn.SSLMode, err, _ = getConfig(spec + ".sslmode")
	if err != nil {
		return nil, err
	}
	return dsn, nil
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
