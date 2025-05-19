package util

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"gopkg.in/ini.v1"
)

const DatabaseVersion = 24

// MetadbVersion is defined at build time via -ldflags.
var MetadbVersion = "(unknown version)"

var DefaultFolioVersion = "refs/tags/v1.7.15"
var FolioVersion = ""
var DefaultReshareVersion = "refs/tags/20230912004531"

const MaximumTypeSizeIndex = 2500

// ModePermRW is the umask "-rw-------".
const ModePermRW = 0600

// ModePermRWX is the umask "-rwx------".
const ModePermRWX = 0700

type RegexList struct {
	String string
	Regex  []*regexp.Regexp
}

func UserPerm(relist *RegexList, table *dbx.Table) bool {
	for _, re := range relist.Regex {
		if re.MatchString(table.String()) {
			return true
		}
	}
	return false
}

func GetFolioVersion() string {
	if FolioVersion == "" {
		return DefaultFolioVersion
	} else {
		return FolioVersion
	}
}

func GetReshareVersion() string {
	return DefaultReshareVersion
}

func GetMetadbVersion() string {
	return MetadbVersion + " (folio=" + GetFolioVersion() + " reshare=" + GetReshareVersion() + ")"
}

func ConfigFileName(datadir string) string {
	return filepath.Join(datadir, "metadb.conf")
}

func SystemDirName(datadir string) string {
	return filepath.Join(datadir, "system")
}

func SystemPIDFileName(datadir string) string {
	return filepath.Join(datadir, "metadb.pid")
}

func SysdbFileName(datadir string) string {
	return filepath.Join(SystemDirName(datadir), "systemdb")
}

// func SystemConfigFileName(datadir string) string {
// 	return filepath.Join(SystemDirName(datadir), "system.conf")
// }

func MatchRegexps(res []*regexp.Regexp, s string) bool {
	var re *regexp.Regexp
	for _, re = range res {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

func CompileRegexps(strs []string) ([]*regexp.Regexp, error) {
	var res []*regexp.Regexp
	for _, s := range strs {
		re, err := regexp.Compile(s)
		if err != nil {
			return nil, fmt.Errorf("compiling regular expression %s: %v", s, err)
		}
		res = append(res, re)
	}
	return res, nil
}

func KafkaMessageString(m *kafka.Message) string {
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "topic partition = %s\n", m.TopicPartition)
	_, _ = fmt.Fprintf(&b, "key = %s\n", m.Key)
	_, _ = fmt.Fprintf(&b, "value = %s\n", m.Value)
	_, _ = fmt.Fprintf(&b, "timestamp = %s\n", m.Timestamp)
	_, _ = fmt.Fprintf(&b, "timestamp type = %s\n", m.TimestampType)
	_, _ = fmt.Fprintf(&b, "opaque = %s\n", m.Opaque)
	_, _ = fmt.Fprintf(&b, "headers = %s\n", m.Headers)
	return b.String()
}

//func RequireFileExists(filename string) error {
//        var err error
//        var ok bool
//        if ok, err = FileExists(filename); err != nil {
//                return err
//        }
//        if !ok {
//                return fmt.Errorf("file not found: %s", filename)
//        }
//        return nil
//}

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

//func ReadRequest(w http.ResponseWriter, r *http.Request, requestStruct interface{}) bool {
//	// Authenticate user.
//	var user string
//	var ok bool
//	if user, ok = HandleBasicAuth(w, r); !ok {
//		return false
//	}
//	_ = user
//	// Read the json request.
//	var body []byte
//	var err error
//	if body, err = ioutil.ReadAll(r.Body); err != nil {
//		HandleError(w, err, http.StatusBadRequest)
//		return false
//	}
//	if err = json.Unmarshal(body, requestStruct); err != nil {
//		HandleError(w, err, http.StatusBadRequest)
//		return false
//	}
//	log.Trace("request %s %v\n", r.RemoteAddr, requestStruct)
//	return true
//}

//func HandleBasicAuth(w http.ResponseWriter, r *http.Request) (string, bool) {
//	var user, password string
//	var ok bool
//	user, password, ok = r.BasicAuth()
//	if !ok {
//		var m = "Unauthorized: Invalid HTTP Basic Authentication"
//		log.Info(m)
//		//w.Header().Set("WWW-Authenticate", "Basic")
//		http.Error(w, m, http.StatusForbidden)
//		return user, false
//	}
//	_ = password
//	//var match bool
//	//var err error
//	//match, err = srv.storage.Authenticate(user, password)
//	//if err != nil {
//	//        var m = "Unauthorized (user '" + user + "')"
//	//        log.Println(m + ": " + err.Error())
//	//        //w.Header().Set("WWW-Authenticate", "Basic")
//	//        http.Error(w, m, http.StatusForbidden)
//	//        return user, false
//	//}
//	/*	if !match {
//			var m = "Unauthorized (user '" + user + "'): " + "Unable to authenticate username/password"
//			log.Info(m)
//			//w.Header().Set("WWW-Authenticate", "Basic")
//			http.Error(w, m, http.StatusForbidden)
//			return user, false
//		}
//	*/
//	return user, true
//}

//func HandleError(w http.ResponseWriter, err error, statusCode int) {
//	log.Error("%s", err)
//	HTTPError(w, err, statusCode)
//}

//func HTTPError(w http.ResponseWriter, err error, code int) {
//	w.Header().Set("Content-Type", "application/json; charset=utf-8")
//	w.Header().Set("X-Content-Type-Options", "nosniff")
//	w.WriteHeader(code)
//	var m = map[string]interface{}{
//		"status": "error",
//		//"message": fmt.Sprintf("%s: %s", http.StatusText(code), err),
//		"message": err.Error(),
//		"code":    code,
//		//"data":    "",
//	}
//	//json.NewEncoder(w).Encode(err)
//	if err = json.NewEncoder(w).Encode(m); err != nil {
//		// TODO error handling
//		_ = err
//	}
//}

// SplitList splits a comma-separated list and trims white space from each element.
func SplitList(list string) []string {
	if list == "" {
		return []string{}
	}
	var sp []string = strings.Split(list, ",")
	var i int
	var s string
	for i, s = range sp {
		sp[i] = strings.TrimSpace(s)
	}
	return sp
}

func ReadConfigDatabase(datadir string) (*dbx.DB, error) {
	cfg, err := ini.Load(ConfigFileName(datadir))
	if err != nil {
		return nil, err
	}
	s := cfg.Section("main")

	checkpointSegmentSize := 100000
	v := s.Key("checkpoint_segment_size").String()
	if v != "" {
		checkpointSegmentSize, err = strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("reading checkpoint_segment_size: parsing %q: invalid syntax", v)
		}
	}

	maxPollInterval := 1800000
	v = s.Key("max_poll_interval").String()
	if v != "" {
		maxPollInterval, err = strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("reading max_poll_interval: parsing %q: invalid syntax", v)
		}
	}

	return &dbx.DB{
		Host:                  s.Key("host").String(),
		Port:                  s.Key("port").String(),
		User:                  s.Key("systemuser").String(),
		Password:              s.Key("systemuser_password").String(),
		SuperUser:             s.Key("superuser").String(),
		SuperPassword:         s.Key("superuser_password").String(),
		DBName:                s.Key("database").String(),
		SSLMode:               s.Key("sslmode").String(),
		CheckpointSegmentSize: checkpointSegmentSize,
		MaxPollInterval:       maxPollInterval,
	}, nil
}

/*func RedactPasswordInURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return "(invalid URI)"
	}
	if u.User == nil {
		return uri
	}
	u.User = url.UserPassword(u.User.Username(), "________")
	return u.String()
}
*/

/*
import "github.com/sethvargo/go-password/password"

func GeneratePassword() (string, error) {
	var res string
	var err error
	if res, err = password.Generate(32, 10, 0, false, true); err != nil {
		return "", err
	}
	return res, nil
}
*/
