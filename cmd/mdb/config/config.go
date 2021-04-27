package config

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"

	"github.com/metadb-project/metadb/cmd/internal/api"
	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/internal/status"
	"github.com/metadb-project/metadb/cmd/mdb/option"
)

func ConfigDatabase(opt *option.ConfigDatabase) error {
	if opt.Name == "" {
		return fmt.Errorf("database connector name not specified")
	}
	if opt.Type == "" || opt.DBHost == "" || opt.DBName == "" || opt.DBUser == "" || opt.DBPassword == "" {
		return fmt.Errorf("insufficient parameters to configure database connector")
	}
	warnNoTLS(opt.NoTLS)
	var rq = api.UpdateDatabaseConnectorRequest{
		Name: opt.Name,
		Config: api.DatabaseConnectorConfig{
			Type:       opt.Type,
			DBHost:     opt.DBHost,
			DBPort:     opt.DBPort,
			DBName:     opt.DBName,
			DBUser:     opt.DBUser,
			DBPassword: opt.DBPassword,
			DBSSLMode:  opt.DBSSLMode,
		},
	}
	var rqj []byte
	var err error
	//if rqj, err = json.Marshal(rq); err != nil {
	if rqj, err = json.MarshalIndent(rq, "", "    "); err != nil {
		return err
	}
	//fmt.Printf("sending: %s\n", string(rqj))

	var conn *tls.Conn
	var transport *http.Transport
	if opt.Host == "" {
		transport = &http.Transport{}
	} else {
		var tlsConfig = http.DefaultTransport.(*http.Transport).TLSClientConfig
		var tlsClientConfig *tls.Config
		if opt.TLSSkipVerify {
			tlsClientConfig = &tls.Config{InsecureSkipVerify: true}
		}
		transport = &http.Transport{
			TLSClientConfig: tlsClientConfig,
			DialTLS: func(network, addr string) (net.Conn, error) {
				conn, err = tls.Dial(network, addr, tlsConfig)
				return conn, err
			},
		}
	}
	var client = &http.Client{Transport: transport}
	var remote string
	if opt.Host == "" {
		remote = "http://127.0.0.1:" + opt.AdminPort
	} else {
		if opt.NoTLS {
			remote = "http://" + opt.Host + ":" + opt.AdminPort
		} else {
			remote = "https://" + opt.Host + ":" + opt.AdminPort
		}
	}
	var httprq *http.Request
	if httprq, err = http.NewRequest("POST", remote+"/databases", bytes.NewBuffer(rqj)); err != nil {
		return err
	}
	httprq.SetBasicAuth("admin", "admin")
	httprq.Header.Set("Content-Type", "application/json")
	var hrs *http.Response
	if hrs, err = client.Do(httprq); err != nil {
		return err
	}
	if conn != nil {
		// verbose output
		var v uint16 = conn.ConnectionState().Version
		eout.Trace("protocol version: %d,%d", (v>>8)&255, v&255)
		var s string
		switch v {
		case 0x0300:
			s = "SSL (deprecated)"
		case 0x0301:
			s = "TLS 1.0 (deprecated)"
		case 0x0302:
			s = "TLS 1.1 (deprecated)"
		case 0x0303:
			s = "TLS 1.2"
		case 0x0304:
			s = "TLS 1.3"
		default:
			s = fmt.Sprintf("unknown version: { %d, %d }", (v>>8)&255, v&255)
		}
		eout.Verbose("TLS/SSL protocol: %s", s)
	} else {
		eout.Verbose("no TLS/SSL protocol")
	}
	if hrs.StatusCode != http.StatusCreated {
		//eout.Error("status code: %d", hrs.StatusCode)
		//if hrs.StatusCode == http.StatusUnauthorized {
		//        fmt.Println("Server at '" + remote + "' did not accept the username/password")
		//}

		// json.NewEncoder(w).Encode(m)
		//return responseBodyError(hrs)
		var m map[string]interface{}
		json.NewDecoder(hrs.Body).Decode(&m)
		return fmt.Errorf("%v", m["message"])
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
	warnNoTLS(opt.NoTLS)
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
	var rqj []byte
	var err error
	//if rqj, err = json.Marshal(rq); err != nil {
	if rqj, err = json.MarshalIndent(rq, "", "    "); err != nil {
		return err
	}
	//fmt.Printf("sending: %s\n", string(rqj))

	var conn *tls.Conn
	var transport *http.Transport
	if opt.Host == "" {
		transport = &http.Transport{}
	} else {
		var tlsConfig = http.DefaultTransport.(*http.Transport).TLSClientConfig
		var tlsClientConfig *tls.Config
		if opt.TLSSkipVerify {
			tlsClientConfig = &tls.Config{InsecureSkipVerify: true}
		}
		transport = &http.Transport{
			TLSClientConfig: tlsClientConfig,
			DialTLS: func(network, addr string) (net.Conn, error) {
				conn, err = tls.Dial(network, addr, tlsConfig)
				return conn, err
			},
		}
	}
	var client = &http.Client{Transport: transport}
	var remote string
	if opt.Host == "" {
		remote = "http://127.0.0.1:" + opt.AdminPort
	} else {
		if opt.NoTLS {
			remote = "http://" + opt.Host + ":" + opt.AdminPort
		} else {
			remote = "https://" + opt.Host + ":" + opt.AdminPort
		}
	}
	var httprq *http.Request
	if httprq, err = http.NewRequest("POST", remote+"/sources", bytes.NewBuffer(rqj)); err != nil {
		return err
	}
	httprq.SetBasicAuth("admin", "admin")
	httprq.Header.Set("Content-Type", "application/json")
	var hrs *http.Response
	if hrs, err = client.Do(httprq); err != nil {
		return err
	}
	if conn != nil {
		// verbose output
		var v uint16 = conn.ConnectionState().Version
		eout.Trace("protocol version: %d,%d", (v>>8)&255, v&255)
		var s string
		switch v {
		case 0x0300:
			s = "SSL (deprecated)"
		case 0x0301:
			s = "TLS 1.0 (deprecated)"
		case 0x0302:
			s = "TLS 1.1 (deprecated)"
		case 0x0303:
			s = "TLS 1.2"
		case 0x0304:
			s = "TLS 1.3"
		default:
			s = fmt.Sprintf("unknown version: { %d, %d }", (v>>8)&255, v&255)
		}
		eout.Verbose("TLS/SSL protocol: %s", s)
	} else {
		eout.Verbose("no TLS/SSL protocol")
	}
	if hrs.StatusCode != http.StatusCreated {
		//eout.Error("status code: %d", hrs.StatusCode)
		//if hrs.StatusCode == http.StatusUnauthorized {
		//        fmt.Println("Server at '" + remote + "' did not accept the username/password")
		//}

		// json.NewEncoder(w).Encode(m)
		//return responseBodyError(hrs)
		var m map[string]interface{}
		json.NewDecoder(hrs.Body).Decode(&m)
		return fmt.Errorf("%v", m["message"])
	}

	return nil
}

func Status(opt *option.Status) error {
	warnNoTLS(opt.NoTLS)
	var rq = api.GetStatusRequest{}
	var rqj []byte
	var err error
	//if rqj, err = json.Marshal(rq); err != nil {
	if rqj, err = json.MarshalIndent(rq, "", "    "); err != nil {
		return err
	}
	//fmt.Printf("sending: %s\n", string(rqj))

	var conn *tls.Conn
	var transport *http.Transport
	if opt.Host == "" {
		transport = &http.Transport{}
	} else {
		var tlsConfig = http.DefaultTransport.(*http.Transport).TLSClientConfig
		var tlsClientConfig *tls.Config
		if opt.TLSSkipVerify {
			tlsClientConfig = &tls.Config{InsecureSkipVerify: true}
		}
		transport = &http.Transport{
			TLSClientConfig: tlsClientConfig,
			DialTLS: func(network, addr string) (net.Conn, error) {
				conn, err = tls.Dial(network, addr, tlsConfig)
				return conn, err
			},
		}
	}
	var client = &http.Client{Transport: transport}
	var remote string
	if opt.Host == "" {
		remote = "http://127.0.0.1:" + opt.AdminPort
	} else {
		if opt.NoTLS {
			remote = "http://" + opt.Host + ":" + opt.AdminPort
		} else {
			remote = "https://" + opt.Host + ":" + opt.AdminPort
		}
	}
	var httprq *http.Request
	if httprq, err = http.NewRequest("GET", remote+"/status", bytes.NewBuffer(rqj)); err != nil {
		return err
	}
	httprq.SetBasicAuth("admin", "admin")
	httprq.Header.Set("Content-Type", "application/json")
	var hrs *http.Response
	if hrs, err = client.Do(httprq); err != nil {
		return err
	}
	if conn != nil {
		// verbose output
		var v uint16 = conn.ConnectionState().Version
		eout.Trace("protocol version: %d,%d", (v>>8)&255, v&255)
		var s string
		switch v {
		case 0x0300:
			s = "SSL (deprecated)"
		case 0x0301:
			s = "TLS 1.0 (deprecated)"
		case 0x0302:
			s = "TLS 1.1 (deprecated)"
		case 0x0303:
			s = "TLS 1.2"
		case 0x0304:
			s = "TLS 1.3"
		default:
			s = fmt.Sprintf("unknown version: { %d, %d }", (v>>8)&255, v&255)
		}
		eout.Verbose("TLS/SSL protocol: %s", s)
	} else {
		eout.Verbose("no TLS/SSL protocol")
	}
	if hrs.StatusCode != http.StatusOK {
		//eout.Error("status code: %d", hrs.StatusCode)
		//if hrs.StatusCode == http.StatusUnauthorized {
		//        fmt.Println("Server at '" + remote + "' did not accept the username/password")
		//}

		// json.NewEncoder(w).Encode(m)
		//return responseBodyError(hrs)
		var m map[string]interface{}
		json.NewDecoder(hrs.Body).Decode(&m)
		return fmt.Errorf("%v", m["message"])
	}

	respbody, err := ioutil.ReadAll(hrs.Body)
	if err != nil {
		return err
	}

	var resp api.GetStatusResponse
	err = json.Unmarshal(respbody, &resp)
	if err != nil {
		return err
	}

	//type GetStatusResponse struct {
	//        Sources   map[string]bool `json:"sources"`
	//        Databases map[string]bool `json:"databases"`
	//}

	//fmt.Printf("%v\n", resp)
	var s string
	var a status.Status
	for s, a = range resp.Databases {
		fmt.Printf("%-9s %-9s %s\n", "database", s, a.GetString())
	}
	for s, a = range resp.Sources {
		fmt.Printf("%-9s %-9s %s\n", "source", s, a.GetString())
	}
	//color.Active.SprintFunc()

	return nil
}

func warnNoTLS(noTLS bool) {
	if noTLS {
		eout.Warning("TLS disabled in connection to server")
	}
}
