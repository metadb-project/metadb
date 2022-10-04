package sysdb

/*
func ReadDataSource(spec string) (string, *sqlx.DSN, error) {
	var err error
	host, _, err := getConfig(spec + ".host")
	if err != nil {
		return "", nil, err
	}
	port, _, err := getConfig(spec + ".port")
	if err != nil {
		return "", nil, err
	}
	dbname, _, err := getConfig(spec + ".dbname")
	if err != nil {
		return "", nil, err
	}
	user, _, err := getConfig(spec + ".adminuser")
	if err != nil {
		return "", nil, err
	}
	password, _, err := getConfig(spec + ".adminpassword")
	if err != nil {
		return "", nil, err
	}
	sslmode, _, err := getConfig(spec + ".sslmode")
	if err != nil {
		return "", nil, err
	}
	account, _, err := getConfig(spec + ".account")
	if err != nil {
		return "", nil, err
	}
	dsn := &sqlx.DSN{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		DBName:   dbname,
		SSLMode:  sslmode,
		Account:  account,
	}
	dbtype, _, err := getConfig(spec + ".type")
	if err != nil {
		return "", nil, err
	}
	return dbtype, dsn, nil
}
*/

/*
func ReadDatabaseConnectors() ([]*DatabaseConnector, error) {
	var cmap map[string]map[string]string
	var err error
	if cmap, err = readConfigMap("db"); err != nil {
		return nil, err
	}
	var dbc []*DatabaseConnector
	var name string
	var conf map[string]string
	for name, conf = range cmap {
		dbc = append(dbc, &DatabaseConnector{
			// Name:            name,
			// Type:            conf["type"],
			DBHost:          conf["host"],
			DBPort:          conf["port"],
			DBName:          conf["dbname"],
			DBAdminUser:     conf["adminuser"],
			DBAdminPassword: conf["adminpassword"],
			DBSuperUser:     conf["superuser"],
			DBSuperPassword: conf["superpassword"],
			// DBUsers:         conf["users"],
			// DBSSLMode:       conf["sslmode"],
			DBAccount:       conf["account"],
		})
	}
	return dbc, nil
}
*/
