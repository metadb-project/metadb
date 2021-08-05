package status

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/internal/status"
	"github.com/metadb-project/metadb/cmd/mdb/option"
	"github.com/metadb-project/metadb/cmd/mdb/util"
)

func Status(opt *option.Status) error {
	var rq = api.GetStatusRequest{}

	var hrs *http.Response
	var err error
	if hrs, err = util.SendRequest(opt.Global, "GET", "/status", rq); err != nil {
		return err
	}

	if hrs.StatusCode != http.StatusOK {
		var m string
		if m, err = util.ReadResponseMessage(hrs); err != nil {
			return err
		}
		return errors.New(m)
	}

	var stat api.GetStatusResponse
	if err = util.ReadResponse(hrs, &stat); err != nil {
		return err
	}

	var s string
	var a status.Status
	for s, a = range stat.Databases {
		fmt.Printf("%-20s %s\n", "db."+s, a.GetString())
	}
	for s, a = range stat.Sources {
		fmt.Printf("%-20s %s\n", "src."+s, a.GetString())
	}

	return nil
}
