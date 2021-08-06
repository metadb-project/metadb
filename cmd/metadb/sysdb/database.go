package sysdb

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
			DBUsers:         conf["users"],
			DBSSLMode:       conf["sslmode"],
		})
	}
	return dbc, nil
}
