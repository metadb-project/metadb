package sysdb

import (
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func ReadSourceConnectors() ([]*SourceConnector, error) {
	var cmap map[string]map[string]string
	var err error
	if cmap, err = readConfigMap("src"); err != nil {
		return nil, err
	}
	var src []*SourceConnector
	var name string
	var conf map[string]string
	for name, conf = range cmap {
		security := conf["security"]
		if security == "" {
			security = "ssl"
		}
		src = append(src, &SourceConnector{
			Name:             name,
			Brokers:          conf["brokers"],
			Security:         security,
			Topics:           util.SplitList(conf["topics"]),
			Group:            conf["group"],
			SchemaPassFilter: util.SplitList(conf["schemapassfilter"]),
			SchemaPrefix:     conf["schemaprefix"],
			Databases:        util.SplitList(conf["dbs"]),
		})
	}
	return src, nil
}
