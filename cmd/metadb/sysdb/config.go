package sysdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/metadb/command"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

func ListConfig(rq *api.ConfigListRequest) (*api.ConfigListResponse, error) {
	mutex.Lock()
	defer mutex.Unlock()

	// read configs
	var rs = &api.ConfigListResponse{}
	var rows *sql.Rows
	var err error
	var where string
	if rq.Attr != "" {
		where = "    WHERE attr = '" + rq.Attr + "'\n"
	}
	var s = "" +
		"SELECT attr, val\n" +
		"    FROM config\n" +
		where +
		"    ORDER BY attr;"
	if rows, err = db.QueryContext(context.TODO(), s); err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var r api.ConfigItem
		if err = rows.Scan(&r.Attr, &r.Val); err != nil {
			return nil, err
		}
		rs.Configs = append(rs.Configs, r)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return rs, nil
}

func UpdateConfig(rq *api.ConfigUpdateRequest) error {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	// TODO allow changing db users
	if strings.HasPrefix(rq.Attr, "db.") && strings.HasSuffix(rq.Attr, ".users") {
		var exists bool
		if _, err, exists = getConfig(rq.Attr); err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("modifying users not yet supported: %s", rq.Attr)
		}
	}

	if err = validateAttr(rq.Attr); err != nil {
		return fmt.Errorf("updating configuration value: %s", err)
	}

	// upsert new config
	var q = fmt.Sprintf(""+
		"INSERT INTO config (attr, val) VALUES ('%s', '%s')\n"+
		"    ON CONFLICT (attr) DO UPDATE SET val = '%s';", rq.Attr, rq.Val, rq.Val)
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("writing configuration: %s: %s", rq.Attr, err)
	}
	// read back the value and return it to the client
	q = fmt.Sprintf(""+
		"SELECT val\n"+
		"    FROM config\n"+
		"    WHERE attr = '%s';", rq.Attr)
	var val string
	if err = db.QueryRowContext(context.TODO(), q).Scan(&val); err != nil {
		return fmt.Errorf("configuration failed: %s: %s", rq.Attr, err)
	}
	//// TMP
	if rq.Attr == "plug.reshare.tenants" && rq.Val != "" {
		command.Tenants = util.SplitList(rq.Val)
	}
	////
	return nil
}

func DeleteConfig(rq *api.ConfigDeleteRequest) (*api.ConfigDeleteResponse, error) {
	mutex.Lock()
	defer mutex.Unlock()

	var err error
	// TODO allow deleting db users
	if strings.HasPrefix(rq.Attr, "db.") && strings.HasSuffix(rq.Attr, ".users") {
		return nil, fmt.Errorf("deleting users not yet supported: %s", rq.Attr)
	}

	if err = validateAttr(rq.Attr); err != nil {
		return nil, fmt.Errorf("deleting configuration value: %s", err)
	}

	// check if a value exists for the attribute
	var exists bool
	/*
		if exists, err = attrExists(rq.Attr); err != nil {
			return nil, err
		}
	*/
	if _, err, exists = getConfig(rq.Attr); err != nil {
		return nil, err
	}
	if exists {
		// delete config
		var err error
		var q = fmt.Sprintf("DELETE FROM config WHERE attr = '%s';", rq.Attr)
		if _, err = db.ExecContext(context.TODO(), q); err != nil {
			return nil, fmt.Errorf("deleting configuration: %s: %s", rq.Attr, err)
		}
		// read back the value and return it to the client
		q = fmt.Sprintf("SELECT val FROM config WHERE attr = '%s';", rq.Attr)
		var val string
		err = db.QueryRowContext(context.TODO(), q).Scan(&val)
		switch {
		case err == sql.ErrNoRows:
			// NOP
		case err != nil:
			return nil, fmt.Errorf("reading configuration: %s: %s", rq.Attr, err)
		default:
			return nil, fmt.Errorf("configuration failed: %s: not deleted", rq.Attr)
		}
	}
	//// TMP
	if rq.Attr == "plug.reshare.tenants" {
		command.Tenants = []string{}
	}
	////
	return &api.ConfigDeleteResponse{AttrNotFound: !exists}, nil
}

func GetConfig(attr string) (string, error, bool) {
	mutex.Lock()
	defer mutex.Unlock()

	return getConfig(attr)
}

func getConfig(attr string) (string, error, bool) {
	// look up config value
	var err error
	var q = fmt.Sprintf(""+
		"SELECT val\n"+
		"    FROM config\n"+
		"    WHERE attr = '%s';", attr)
	var val string
	err = db.QueryRowContext(context.TODO(), q).Scan(&val)
	switch {
	case err == sql.ErrNoRows:
		return "", nil, false
	case err != nil:
		return "", fmt.Errorf("reading configuration: %s: %s", attr, err), false
	default:
		return val, nil, true
	}
}

func readConfigMap(prefix string) (map[string]map[string]string, error) {
	var cmap = make(map[string]map[string]string)
	var rows *sql.Rows
	var err error
	var q = "SELECT attr, val FROM config WHERE attr LIKE '" + prefix + ".%';"
	if rows, err = db.QueryContext(context.TODO(), q); err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var attr, val string
		if err = rows.Scan(&attr, &val); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(attr, prefix+".") {
			continue
		}
		var sp []string = strings.Split(attr, ".")
		if len(sp) < 3 {
			continue
		}
		var name = sp[1]
		var key = sp[2]
		var conf map[string]string
		var ok bool
		if conf, ok = cmap[name]; !ok {
			conf = make(map[string]string)
			cmap[name] = conf
		}
		conf[key] = val
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return cmap, nil
}

func validateAttr(attr string) error {
	// For now, allow all db.* and src.*
	if strings.HasPrefix(attr, "db.") || strings.HasPrefix(attr, "src.") {
		return nil
	}
	if attr != "plug.reshare.tenants" {
		return fmt.Errorf("invalid attribute: %s", attr)
	}
	return nil
}
