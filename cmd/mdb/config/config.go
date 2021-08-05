package config

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/mdb/option"
	"github.com/metadb-project/metadb/cmd/mdb/util"
)

func Config(opt *option.Config) error {
	switch {
	case opt.Delete:
		if opt.Attr == nil || *opt.Attr == "" {
			return fmt.Errorf("attribute not specified")
		}
		return ConfigDelete(opt)
	case opt.List:
		return ConfigList(opt)
	default:
		if opt.Attr == nil || *opt.Attr == "" {
			return fmt.Errorf("attribute not specified")
		}
		if opt.Val == nil {
			return ConfigList(opt)
			//return fmt.Errorf("value not specified")
		}
		return ConfigUpdate(opt)
	}
}

func ConfigDelete(opt *option.Config) error {
	// convert config options to a request
	var rq = &api.ConfigDeleteRequest{Attr: *opt.Attr}
	// send the request
	var httprs *http.Response
	var err error
	if httprs, err = util.SendRequest(opt.Global, "DELETE", "/config", rq); err != nil {
		return err
	}
	// check for error response
	if httprs.StatusCode != http.StatusOK {
		var m string
		if m, err = util.ReadResponseMessage(httprs); err != nil {
			return err
		}
		return fmt.Errorf("%s", m)
	}
	// read response body
	var rs api.ConfigDeleteResponse
	if err = util.ReadResponse(httprs, &rs); err != nil {
		return err
	}
	// print confirmation
	if rs.AttrNotFound {
		eout.Warning("config: attribute %q not found", rq.Attr)
	} else {
		eout.Info("config: deleted %q", rq.Attr)
	}
	return nil
}

func ConfigList(opt *option.Config) error {
	// send the request
	var httprs *http.Response
	var rq *api.ConfigListRequest
	if opt.Attr == nil {
		rq = &api.ConfigListRequest{Attr: ""}
	} else {
		rq = &api.ConfigListRequest{Attr: *opt.Attr}
	}
	var err error
	if httprs, err = util.SendRequest(opt.Global, "GET", "/config", rq); err != nil {
		return err
	}
	// check for error response
	if httprs.StatusCode != http.StatusOK {
		var m string
		if m, err = util.ReadResponseMessage(httprs); err != nil {
			return err
		}
		return fmt.Errorf("%s", m)
	}
	// read response body
	var rs api.ConfigListResponse
	if err = util.ReadResponse(httprs, &rs); err != nil {
		return err
	}
	// print response
	if opt.Attr != nil {
		if len(rs.Configs) == 0 {
			eout.Warning("config: attribute %q not found", rq.Attr)
		} else {
			fmt.Printf("%q\n", rs.Configs[0].Val)
		}
	} else {
		var c api.ConfigItem
		for _, c = range rs.Configs {
			fmt.Printf("%s %q\n", c.Attr, c.Val)
		}
	}
	return nil
}

func ConfigUpdate(opt *option.Config) error {
	// convert config options to a request
	var rq = &api.ConfigUpdateRequest{Attr: *opt.Attr, Val: *opt.Val}
	// send the request
	var httprs *http.Response
	var err error
	if httprs, err = util.SendRequest(opt.Global, "POST", "/config", rq); err != nil {
		return err
	}
	// check for error response
	if httprs.StatusCode != http.StatusCreated {
		var m string
		if m, err = util.ReadResponseMessage(httprs); err != nil {
			return err
		}
		return fmt.Errorf("%s", m)
	}
	// print response
	eout.Info("config: updated %q", rq.Attr)
	return nil
}

func ConfigDatabase(opt *option.ConfigDatabase) error {
	if opt.Name == "" {
		return fmt.Errorf("database connector name not specified")
	}
	if opt.Type == "" || opt.DBHost == "" || opt.DBName == "" || opt.DBAdminUser == "" || opt.DBAdminPassword == "" || opt.DBUsers == "" {
		return fmt.Errorf("insufficient parameters to configure database connector")
	}
	var rq = api.UpdateDatabaseConnectorRequest{
		Name: opt.Name,
		Config: api.DatabaseConnectorConfig{
			Type:            opt.Type,
			DBHost:          opt.DBHost,
			DBPort:          opt.DBPort,
			DBName:          opt.DBName,
			DBAdminUser:     opt.DBAdminUser,
			DBAdminPassword: opt.DBAdminPassword,
			DBUsers:         opt.DBUsers,
			DBSSLMode:       opt.DBSSLMode,
		},
	}

	var hrs *http.Response
	var err error
	if hrs, err = util.SendRequest(opt.Global, "POST", "/databases", rq); err != nil {
		return err
	}

	if hrs.StatusCode != http.StatusCreated {
		var m string
		if m, err = util.ReadResponseMessage(hrs); err != nil {
			return err
		}
		return errors.New(m)
	}

	return nil
}

func ConfigSource(opt *option.ConfigSource) error {
	if opt.Name == "" {
		return fmt.Errorf("source connector name not specified")
	}
	if opt.Brokers == "" {
		return fmt.Errorf("source connector brokers not specified")
	}
	if len(opt.Topics) == 0 {
		return fmt.Errorf("source connector topic not specified")
	}
	if opt.Group == "" {
		return fmt.Errorf("source connector group not specified")
	}
	if len(opt.Databases) == 0 {
		return fmt.Errorf("source connector database not specified")
	}
	if len(opt.Databases) > 1 {
		return fmt.Errorf("multiple databases are not yet supported")
	}
	var rq = api.UpdateSourceConnectorRequest{
		Name: opt.Name,
		Config: api.SourceConnectorConfig{
			Brokers:          opt.Brokers,
			Topics:           opt.Topics,
			Group:            opt.Group,
			SchemaPassFilter: opt.SchemaPassFilter,
			SchemaPrefix:     opt.SchemaPrefix,
			Databases:        opt.Databases,
		},
	}

	var hrs *http.Response
	var err error
	if hrs, err = util.SendRequest(opt.Global, "POST", "/sources", rq); err != nil {
		return err
	}

	if hrs.StatusCode != http.StatusCreated {
		var m string
		if m, err = util.ReadResponseMessage(hrs); err != nil {
			return err
		}
		return errors.New(m)
	}

	return nil
}
