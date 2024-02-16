package user

import (
	"fmt"
	"net/http"

	"github.com/nazgaret/metadb/cmd/internal/api"
	"github.com/nazgaret/metadb/cmd/internal/eout"
	"github.com/nazgaret/metadb/cmd/mdb/option"
	"github.com/nazgaret/metadb/cmd/mdb/util"
)

func User(opt *option.User) error {
	switch {
	case opt.Delete:
		if opt.Name == nil || *opt.Name == "" {
			return fmt.Errorf("user name not specified")
		}
		return UserDelete(opt)
	case opt.List:
		return UserList(opt)
	default:
		if opt.Name == nil || *opt.Name == "" {
			return fmt.Errorf("user name not specified")
		}
		if opt.Tables == nil {
			return UserList(opt)
		}
		return UserUpdate(opt)
	}
}

func UserDelete(opt *option.User) error {
	// convert options to a request
	var rq = &api.UserDeleteRequest{Name: *opt.Name}
	// send the request
	var httprs *http.Response
	var err error
	if httprs, err = util.SendRequest(opt.Global, "DELETE", "/user", rq); err != nil {
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
	var rs api.UserDeleteResponse
	if err = util.ReadResponse(httprs, &rs); err != nil {
		return err
	}
	// print confirmation
	if rs.NameNotFound {
		eout.Warning("user: user name %q not found", rq.Name)
	} else {
		eout.Info("user: deleted permissions for %q", rq.Name)
	}
	eout.Info("user: restart server to update all permissions")
	return nil
}

func UserList(opt *option.User) error {
	// send the request
	var httprs *http.Response
	var rq *api.UserListRequest
	if opt.Name == nil {
		rq = &api.UserListRequest{Name: ""}
	} else {
		rq = &api.UserListRequest{Name: *opt.Name}
	}
	var err error
	if httprs, err = util.SendRequest(opt.Global, "GET", "/user", rq); err != nil {
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
	var rs api.UserListResponse
	if err = util.ReadResponse(httprs, &rs); err != nil {
		return err
	}
	// print response
	if opt.Name != nil {
		// if len(rs.Users) == 0 {
		if len(rs.Users) == 0 || rs.Users[0].Tables == "" {
			eout.Warning("user: user name %q not found", rq.Name)
		} else {
			// fmt.Printf("%q\n", rs.Users[0].Tables)
			fmt.Printf("%s\n", rs.Users[0].Name)
		}
	} else {
		var c api.UserItem
		for _, c = range rs.Users {
			// fmt.Printf("%s %q\n", c.Name, c.Tables)
			fmt.Printf("%s\n", c.Name)
		}
	}
	return nil
}

func UserUpdate(opt *option.User) error {
	// convert options to a request
	var rq = &api.UserUpdateRequest{
		Name:     *opt.Name,
		Tables:   *opt.Tables,
		Create:   opt.Create,
		Password: opt.Password,
	}
	// send the request
	var httprs *http.Response
	var err error
	if httprs, err = util.SendRequest(opt.Global, "POST", "/user", rq); err != nil {
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
	if opt.Create {
		eout.Info("user: created %q", rq.Name)
	} else {
		eout.Info("user: updated %q", rq.Name)
	}
	eout.Info("user: restart server to update all permissions")
	return nil
}
