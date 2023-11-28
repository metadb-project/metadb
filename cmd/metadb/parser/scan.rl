package parser

import (
	"github.com/metadb-project/metadb/cmd/metadb/ast"
)

%%{ 
	machine sql;
	write data;
	access lex.;
	variable p lex.p;
	variable pe lex.pe;

	identifier = [A-Za-z_][0-9A-Za-z_.]*;
	sliteral = ['][^']*['];
}%%

type lexer struct {
	data []byte
	p, pe, cs int
	ts, te, act int

	err string
	str string
	optlist []ast.Option
	node ast.Node
	pass bool
}

func newLexer(data []byte) *lexer {
	lex := &lexer{ 
		data: data,
		pe: len(data),
	}
	%% write init;
	return lex
}

func (lex *lexer) Lex(out *yySymType) int {
	eof := lex.pe
	tok := 0
	%%{ 
		main := |*
			';' => { tok = ';'; fbreak; };
			',' => { tok = ','; fbreak; };
			'(' => { tok = '('; fbreak; };
			')' => { tok = ')'; fbreak; };
			'true'i => { tok = TRUE; fbreak; };
			'false'i => { tok = FALSE; fbreak; };
			'select'i => { tok = SELECT; fbreak; };
			'create'i => { tok = CREATE; fbreak; };
			'alter'i => { tok = ALTER; fbreak; };
			'data'i => { tok = DATA; fbreak; };
			'source'i => { tok = SOURCE; fbreak; };
			'origin'i => { tok = ORIGIN; fbreak; };
			'options'i => { tok = OPTIONS; fbreak; };
			'add'i => { tok = ADD; fbreak; };
			'set'i => { tok = SET; fbreak; };
			'drop'i => { tok = DROP; fbreak; };
			'type'i => { tok = TYPE; fbreak; };
			'authorize'i => { tok = AUTHORIZE; fbreak; };
			'on'i => { tok = ON; fbreak; };
			'all'i => { tok = ALL; fbreak; };
			'table'i => { tok = TABLE; fbreak; };
			'tables'i => { tok = TABLES; fbreak; };
			'in'i => { tok = IN; fbreak; };
			'to'i => { tok = TO; fbreak; };
			'user'i => { tok = USER; fbreak; };
			'with'i => { tok = WITH; fbreak; };
			'mapping'i => { tok = MAPPING; fbreak; };
			'list'i => { tok = LIST; fbreak; };
			'refresh'i => { tok = REFRESH; fbreak; };
			'inferred'i => { tok = INFERRED; fbreak; };
			'column'i => { tok = COLUMN; fbreak; };
			'types'i => { tok = TYPES; fbreak; };
			'version'i => { out.str = "version"; tok = VERSION; fbreak; };
			identifier => { out.str = string(lex.data[lex.ts:lex.te]); tok = IDENT; fbreak; };
			sliteral => { out.str = string(lex.data[lex.ts+1:lex.te-1]); tok = SLITERAL; fbreak; };
			digit+ => { out.str = string(lex.data[lex.ts:lex.te]); tok = NUMBER; fbreak; };
			space;
		*|;

		write exec;
	}%%

	return tok;
}

func (lex *lexer) Error(e string) {
	lex.err = e
}
