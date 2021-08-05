package sysdb

import (
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/log"
)

func ReadDatabaseConnectors() ([]*DatabaseConnector, error) {
	mutex.Lock()
	defer mutex.Unlock()

	var cmap = make(map[string]map[string]string)
	var err error
	if cmap, err = readConfigMap("db"); err != nil {
		return nil, err
	}
	var dbc []*DatabaseConnector
	var name string
	var conf map[string]string
	for name, conf = range cmap {
		if strings.TrimSpace(conf["users"]) == "" {
			log.Error("db.%s.users is undefined", name)
			continue
		}
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
