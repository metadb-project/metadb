package util

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/metadb-project/metadb/cmd/metadb/log"
)

// ModePermRW is the umask "-rw-------".
const ModePermRW = 0600

// ModePermRWX is the umask "-rwx------".
const ModePermRWX = 0700

func SystemDirName(datadir string) string {
	return filepath.Join(datadir, "system")
}

func SystemPIDFileName(datadir string) string {
	return filepath.Join(datadir, "metadb.pid")
}

func SysdbFileName(datadir string) string {
	return filepath.Join(SystemDirName(datadir), "system.db")
}

func JoinSchemaTable(schema, table string) string {
	if schema == "" {
		return fmt.Sprintf("\"%s\"", table)
	} else {
		return fmt.Sprintf("\"%s\".\"%s\"", schema, table)
	}
}

func MatchRegexps(res []*regexp.Regexp, s string) bool {
	var re *regexp.Regexp
	for _, re = range res {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

func CompileRegexps(strs []string) []*regexp.Regexp {
	var res []*regexp.Regexp
	var str string
	for _, str = range strs {
		var re *regexp.Regexp = regexp.MustCompile(str)
		res = append(res, re)
	}
	return res
}

func KafkaMessageString(m *kafka.Message) string {
	var b strings.Builder
	fmt.Fprintf(&b, "topic partition = %s\n", m.TopicPartition)
	fmt.Fprintf(&b, "key = %s\n", m.Key)
	fmt.Fprintf(&b, "value = %s\n", m.Value)
	fmt.Fprintf(&b, "timestamp = %s\n", m.Timestamp)
	fmt.Fprintf(&b, "timestamp type = %s\n", m.TimestampType)
	fmt.Fprintf(&b, "opaque = %s\n", m.Opaque)
	fmt.Fprintf(&b, "headers = %s\n", m.Headers)
	return b.String()
}

func PostgresEncodeString(str string, e bool) string {
	var b strings.Builder
	if e {
		b.WriteRune('E')
	}
	b.WriteRune('\'')
	for _, c := range str {
		switch c {
		case '\\':
			b.WriteString("\\\\")
		case '\'':
			b.WriteString("''")
		case '\b':
			b.WriteString("\\b")
		case '\f':
			b.WriteString("\\f")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		default:
			b.WriteRune(c)
		}
	}
	b.WriteRune('\'')
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

func ReadRequest(w http.ResponseWriter, r *http.Request, requestStruct interface{}) bool {
	// Authenticate user.
	var user string
	var ok bool
	if user, ok = HandleBasicAuth(w, r); !ok {
		return false
	}
	_ = user
	// Read the json request.
	var body []byte
	var err error
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		HandleError(w, err, http.StatusBadRequest)
		return false
	}
	if err = json.Unmarshal(body, requestStruct); err != nil {
		HandleError(w, err, http.StatusBadRequest)
		return false
	}
	log.Trace("request %s %v\n", r.RemoteAddr, requestStruct)
	return true
}

func HandleBasicAuth(w http.ResponseWriter, r *http.Request) (
	string, bool) {
	var user, password string
	var ok bool
	user, password, ok = r.BasicAuth()
	if !ok {
		var m = "Unauthorized: Invalid HTTP Basic Authentication"
		log.Info(m)
		//w.Header().Set("WWW-Authenticate", "Basic")
		http.Error(w, m, http.StatusForbidden)
		return user, false
	}
	_ = password
	var match bool = true
	//var err error
	//match, err = srv.storage.Authenticate(user, password)
	//if err != nil {
	//        var m = "Unauthorized (user '" + user + "')"
	//        log.Println(m + ": " + err.Error())
	//        //w.Header().Set("WWW-Authenticate", "Basic")
	//        http.Error(w, m, http.StatusForbidden)
	//        return user, false
	//}
	if !match {
		var m = "Unauthorized (user '" + user + "'): " +
			"Unable to authenticate username/password"
		log.Info(m)
		//w.Header().Set("WWW-Authenticate", "Basic")
		http.Error(w, m, http.StatusForbidden)
		return user, false
	}
	return user, true
}

func HandleError(w http.ResponseWriter, err error, statusCode int) {
	log.Error("%s", err)
	HTTPError(w, err, statusCode)
}

func HTTPError(w http.ResponseWriter, err error, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	var m = map[string]interface{}{
		"status": "error",
		//"message": fmt.Sprintf("%s: %s", http.StatusText(code), err),
		"message": err.Error(),
		"code":    code,
		//"data":    "",
	}
	//json.NewEncoder(w).Encode(err)
	if err = json.NewEncoder(w).Encode(m); err != nil {
		// TODO error handling
		_ = err
	}
}

// SplitList splits a comma-separated list and trims white space from each element.
func SplitList(list string) []string {
	var sp []string = strings.Split(list, ",")
	var i int
	var s string
	for i, s = range sp {
		sp[i] = strings.TrimSpace(s)
	}
	return sp
}
