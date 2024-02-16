package enable

import (
	"errors"
	"net/http"
	"strings"

	"github.com/nazgaret/metadb/cmd/internal/api"
	"github.com/nazgaret/metadb/cmd/internal/eout"
	"github.com/nazgaret/metadb/cmd/mdb/option"
	"github.com/nazgaret/metadb/cmd/mdb/util"
)

func Enable(opt *option.Enable) error {
	var rq = &api.EnableRequest{Connectors: opt.Connectors}
	// send the request
	var httprs *http.Response
	var err error
	if httprs, err = util.SendRequest(opt.Global, "POST", "/enable", rq); err != nil {
		return err
	}
	// check for error response
	if httprs.StatusCode != http.StatusCreated {
		var m string
		if m, err = util.ReadResponseMessage(httprs); err != nil {
			return err
		}
		return errors.New(m)
	}
	// print confirmation
	eout.Info("enabled: %s", strings.Join(rq.Connectors, " "))
	return nil
}
