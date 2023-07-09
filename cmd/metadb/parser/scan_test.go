package parser

/*
type sym struct {
	r int
	y yySymType
}

func lex(input string) []sym {
	rs := make([]sym, 0)
	l := newLexer([]byte(input))
	y := yySymType{}
	for {
		r := l.Lex(&y)
		if r == 0 {
			break
		}
		rs = append(rs, sym{r, y})
	}
	return rs
}

func TestLexSelectVersion(t *testing.Table) {
	s := "select version();"
	// want := []sym{{SELECT, ""}, {IDENT, "version"}, {OPENPAREN, ""}, {CLOSEPAREN, ""}, {SEMICOLON, ""}}
	got := lex(s)
	// if got != want {
	if len(got) != 5 || got[0].r != SELECT || got[1].r != IDENT || got[1].y.str != "version" || got[2].r != '(' || got[3].r != ')' || got[4].r != ';' {
		t.Errorf("got %v; want [%d %d %d %d %d]", got, SELECT, IDENT, '(', ')', ';')
	}
}
*/
