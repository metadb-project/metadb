package util

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
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

//func CloseRows(rows *sql.Rows) {
//        _ = rows.Close()
//}

//func Rollback(tx *sql.Tx) {
//        _ = tx.Rollback()
//}
