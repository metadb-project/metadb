package util

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"

	"github.com/metadb-project/metadb/cmd/internal/eout"
	"github.com/metadb-project/metadb/cmd/mdb/option"
)

// TODO duplicate
// ModePermRW is the umask "-rw-------".
const ModePermRW = 0600

// TODO duplicate
// ModePermRWX is the umask "-rwx------".
const ModePermRWX = 0700

// TODO duplicate
// FileExists returns true if f is an existing file or directory.
func FileExists(f string) (bool, error) {
	_, err := os.Stat(f)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func SendRequest(opt option.Global, method, url string, requestStruct interface{}) (*http.Response, error) {
	WarnNoTLS(opt.NoTLS)
	var rqj []byte
	var err error
	if rqj, err = json.Marshal(requestStruct); err != nil {
		return nil, err
	}

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
	if httprq, err = http.NewRequest(method, remote+url, bytes.NewBuffer(rqj)); err != nil {
		return nil, err
	}
	httprq.SetBasicAuth("admin", "admin")
	httprq.Header.Set("Content-Type", "application/json")
	var hrs *http.Response
	if hrs, err = client.Do(httprq); err != nil {
		return nil, err
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
	return hrs, nil
}

func ReadResponse(httpResponse *http.Response, responseStruct interface{}) error {
	var body []byte
	var err error
	if body, err = ioutil.ReadAll(httpResponse.Body); err != nil {
		return err
	}
	if err = json.Unmarshal(body, responseStruct); err != nil {
		return err
	}
	return nil
}

func ReadResponseMessage(httpResponse *http.Response) (string, error) {
	var m map[string]interface{}
	var err error
	if m, err = ReadResponseMap(httpResponse); err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", m["message"]), nil
}

func ReadResponseMap(httpResponse *http.Response) (map[string]interface{}, error) {
	var m map[string]interface{}
	var err error
	if err = json.NewDecoder(httpResponse.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("decoding server response: %s", err)
	}
	return m, nil
}

func WarnNoTLS(noTLS bool) {
	if noTLS {
		eout.Warning("TLS disabled in connection to server")
	}
}
