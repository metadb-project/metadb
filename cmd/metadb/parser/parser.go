package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/ast"
)

//go:generate ragel -Z -G2 -o scan.go scan.rl
//go:generate goyacc -l -o gram.go gram.y

func WriteErrorContext(query string, position int) string {
	var b strings.Builder
	// Scan to position, counting the number of newlines.
	var pos, line, markline, linepos int
	for _, r := range query {
		if pos >= position {
			markline = line
			break
		}
		if r == '\n' {
			line++
			linepos = 0
			pos++
			continue
		}
		pos++
		linepos++
	}
	s := fmt.Sprintf("LINE %d: ", markline+1)
	margin := len(s)
	b.WriteString(s)
	// Scan again, printing the line containing position.
	var w bool
	pos = 0
	line = 0
	for _, r := range query {
		if line >= markline {
			if r != '\n' {
				b.WriteRune(r)
			}
			w = true
		}
		if r == '\n' {
			if w {
				break
			}
			line++
		}
		pos++
	}
	b.WriteRune('\n')
	// Write pointer at linepos.
	for i := 0; i < margin; i++ {
		b.WriteRune(' ')
	}
	for i := 0; i < linepos; i++ {
		b.WriteRune(' ')
	}
	b.WriteRune('^')
	return b.String()
}

func errorMessage(l *lexer) error {
	ts := l.ts
	te := l.te
	if ts == te {
		te++
	}
	s := fmt.Sprintf("%s at or near %q\n%s", l.err, l.data[ts:te], WriteErrorContext(string(l.data), l.ts))
	return errors.New(s)
}

func Parse(input string) (ast.Node, error, bool) {
	l := newLexer([]byte(input))
	e := yyParse(l)
	var msg error
	if e != 0 {
		msg = errorMessage(l)
	}
	return l.node, msg, l.pass
}
