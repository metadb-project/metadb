// Code generated by goyacc -l -o gram.go gram.y. DO NOT EDIT.
package parser

import __yyfmt__ "fmt"

import (
	"github.com/metadb-project/metadb/cmd/metadb/ast"
)

type yySymType struct {
	yys     int
	str     string
	optlist []ast.Option
	node    ast.Node
	pass    bool
}

const SELECT = 57346
const CONSISTENCY = 57347
const CREATE = 57348
const ALTER = 57349
const DATA = 57350
const SOURCE = 57351
const ORIGIN = 57352
const OPTIONS = 57353
const USER = 57354
const AUTHORIZE = 57355
const DEAUTHORIZE = 57356
const ON = 57357
const ALL = 57358
const TABLE = 57359
const TABLES = 57360
const IN = 57361
const TO = 57362
const WITH = 57363
const MAPPING = 57364
const LIST = 57365
const REFRESH = 57366
const INFERRED = 57367
const COLUMN = 57368
const TYPES = 57369
const TYPE = 57370
const TRUE = 57371
const FALSE = 57372
const VERIFY = 57373
const FOR = 57374
const FROM = 57375
const PATH = 57376
const VERSION = 57377
const ADD = 57378
const SET = 57379
const DROP = 57380
const IDENT = 57381
const NUMBER = 57382
const SLITERAL = 57383

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"SELECT",
	"CONSISTENCY",
	"CREATE",
	"ALTER",
	"DATA",
	"SOURCE",
	"ORIGIN",
	"OPTIONS",
	"USER",
	"AUTHORIZE",
	"DEAUTHORIZE",
	"ON",
	"ALL",
	"TABLE",
	"TABLES",
	"IN",
	"TO",
	"WITH",
	"MAPPING",
	"LIST",
	"REFRESH",
	"INFERRED",
	"COLUMN",
	"TYPES",
	"TYPE",
	"TRUE",
	"FALSE",
	"VERIFY",
	"FOR",
	"FROM",
	"PATH",
	"VERSION",
	"ADD",
	"SET",
	"DROP",
	"IDENT",
	"NUMBER",
	"SLITERAL",
	"';'",
	"'('",
	"')'",
	"','",
}

var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

var yyExca = [...]int8{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 145

var yyAct = [...]uint8{
	75, 88, 101, 73, 109, 72, 127, 87, 92, 74,
	112, 113, 86, 139, 136, 87, 135, 108, 91, 83,
	23, 80, 10, 13, 76, 71, 64, 37, 56, 24,
	25, 46, 48, 54, 50, 138, 134, 89, 40, 26,
	27, 131, 38, 130, 57, 85, 59, 28, 99, 58,
	63, 69, 65, 21, 15, 22, 47, 68, 121, 70,
	40, 104, 103, 102, 38, 77, 55, 41, 60, 40,
	84, 43, 45, 38, 137, 129, 94, 93, 90, 82,
	32, 81, 97, 67, 44, 66, 53, 96, 52, 31,
	34, 98, 29, 107, 33, 106, 30, 79, 110, 119,
	111, 118, 105, 51, 49, 62, 42, 117, 36, 35,
	1, 39, 114, 115, 116, 120, 122, 123, 124, 125,
	126, 100, 128, 105, 78, 95, 20, 61, 12, 19,
	132, 133, 18, 7, 6, 9, 8, 17, 16, 14,
	11, 5, 4, 3, 2,
}

var yyPact = [...]int16{
	16, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	84, -1000, -1000, 72, -1000, 82, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 105, 104, 3, 42, 101, 62,
	34, 3, 95, 3, 94, 73, 71, -9, -1000, -1000,
	-1000, 40, -14, 3, 17, 3, 47, -1000, 98, 3,
	-16, 3, 69, 67, -1000, 30, -1000, 23, 3, -17,
	3, -18, 39, 86, -1000, -21, 63, 61, -23, 3,
	12, -1000, -30, -1000, -4, -1000, -1000, 3, -24, -35,
	-1000, 58, 57, -1000, 76, 65, -1000, 3, -1000, -1000,
	20, -1000, 25, 87, 85, -25, -39, 3, -1000, 3,
	-34, -1000, 3, 3, 3, -4, 92, 90, -1000, 3,
	32, -1000, -1000, 25, -1000, -4, -4, -1000, 3, 3,
	-38, 3, -1000, -1000, -1000, 55, 10, -1000, 7, 3,
	3, -5, -26, -28, 54, -1000, -1000, -6, -29, -1000,
}

var yyPgo = [...]uint8{
	0, 144, 143, 142, 141, 140, 139, 138, 137, 136,
	135, 134, 133, 132, 129, 128, 127, 126, 125, 124,
	5, 121, 3, 2, 9, 1, 0, 111, 110,
}

var yyR1 = [...]int8{
	0, 28, 1, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 3, 4, 11, 12, 9, 9, 10, 15,
	16, 5, 6, 18, 19, 20, 20, 21, 21, 22,
	23, 23, 23, 23, 24, 25, 7, 8, 13, 14,
	17, 26, 26, 27,
}

var yyR2 = [...]int8{
	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 8, 15, 5, 6, 3, 4, 5,
	5, 6, 5, 4, 4, 1, 3, 1, 3, 2,
	2, 3, 3, 2, 1, 1, 12, 12, 3, 5,
	3, 1, 1, 1,
}

var yyChk = [...]int16{
	-1000, -28, -1, -2, -3, -4, -11, -12, -9, -10,
	6, -5, -15, 7, -6, 38, -7, -8, -13, -14,
	-17, 37, 39, 4, 13, 14, 23, 24, 31, 8,
	12, 17, 8, 12, 8, 4, 4, -26, 39, -27,
	35, 25, 5, 9, 22, 10, -26, 22, -26, 9,
	-26, 9, 15, 15, 42, 26, 42, -26, 32, -26,
	21, -16, 7, -26, 42, -26, 16, 16, 27, 28,
	-26, 42, -20, -22, -24, -26, 42, 26, -19, 11,
	42, 18, 18, 42, -26, 33, 42, 45, -25, 41,
	-26, 42, 43, 19, 19, -18, 11, 17, -22, 28,
	-21, -23, 38, 37, 36, -24, 8, 8, 42, 43,
	-26, -26, 44, 45, -24, -24, -24, -25, 9, 9,
	-20, 26, -23, -25, -25, -26, -26, 44, -26, 20,
	33, 34, -26, -26, 41, 42, 42, 20, 41, 42,
}

var yyDef = [...]int8{
	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
	19, 20, 21, 22, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 51, 52,
	53, 0, 0, 0, 0, 0, 0, 27, 0, 0,
	0, 0, 0, 0, 48, 0, 50, 0, 0, 0,
	0, 0, 0, 0, 28, 0, 0, 0, 0, 0,
	0, 25, 0, 35, 0, 44, 29, 0, 0, 0,
	32, 0, 0, 49, 0, 0, 26, 0, 39, 45,
	0, 31, 0, 0, 0, 0, 0, 0, 36, 0,
	0, 37, 0, 0, 0, 0, 0, 0, 23, 0,
	0, 30, 34, 0, 40, 0, 0, 43, 0, 0,
	0, 0, 38, 41, 42, 0, 0, 33, 0, 0,
	0, 0, 0, 0, 0, 46, 47, 0, 0, 24,
}

var yyTok1 = [...]int8{
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	43, 44, 3, 3, 45, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 42,
}

var yyTok2 = [...]int8{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
}

var yyTok3 = [...]int8{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := int(yyPact[state])
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && int(yyChk[int(yyAct[n])]) == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || int(yyExca[i+1]) != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := int(yyExca[i])
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = int(yyTok1[0])
		goto out
	}
	if char < len(yyTok1) {
		token = int(yyTok1[char])
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = int(yyTok2[char-yyPrivate])
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = int(yyTok3[i+0])
		if token == char {
			token = int(yyTok3[i+1])
			goto out
		}
	}

out:
	if token == 0 {
		token = int(yyTok2[1]) /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = int(yyPact[yystate])
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = int(yyAct[yyn])
	if int(yyChk[yyn]) == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = int(yyDef[yystate])
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && int(yyExca[xi+1]) == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = int(yyExca[xi+0])
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = int(yyExca[xi+1])
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = int(yyPact[yyS[yyp].yys]) + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = int(yyAct[yyn]) /* simulate a shift of "error" */
					if int(yyChk[yystate]) == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yyrcvr.char = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= int(yyR2[yyn])
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = int(yyR1[yyn])
	yyg := int(yyPgo[yyn])
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = int(yyAct[yyg])
	} else {
		yystate = int(yyAct[yyj])
		if int(yyChk[yystate]) != -yyn {
			yystate = int(yyAct[yyg])
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yylex.(*lexer).node = yyDollar[1].node
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 6:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yylex.(*lexer).pass = true
			// $$ = nil
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yylex.(*lexer).pass = true
			// $$ = nil
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yylex.(*lexer).pass = true
			// $$ = nil
		}
	case 15:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 16:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 17:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 18:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 19:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.node = yyDollar[1].node
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yylex.(*lexer).pass = true
			// $$ = nil
		}
	case 21:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yylex.(*lexer).pass = true
			// $$ = nil
		}
	case 22:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yylex.(*lexer).pass = true
			yyVAL.node = &ast.SelectStmt{}
		}
	case 23:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.node = &ast.CreateDataSourceStmt{DataSourceName: yyDollar[4].str, TypeName: yyDollar[6].str, Options: yyDollar[7].optlist}
		}
	case 24:
		yyDollar = yyS[yypt-15 : yypt+1]
		{
			yyVAL.node = &ast.CreateDataMappingStmt{TypeName: yyDollar[5].str, TableName: yyDollar[8].str, ColumnName: yyDollar[10].str, Path: yyDollar[12].str, TargetIdentifier: yyDollar[14].str}
		}
	case 25:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.node = &ast.CreateDataOriginStmt{OriginName: yyDollar[4].str}
		}
	case 26:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.node = &ast.CreateUserStmt{UserName: yyDollar[3].str, Options: yyDollar[5].optlist}
		}
	case 27:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yylex.(*lexer).pass = true
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.node = &ast.DropUserStmt{UserName: yyDollar[3].str}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.node = &ast.AlterTableStmt{TableName: yyDollar[3].str, Cmd: (yyDollar[4].node).(*ast.AlterTableCmd)}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.node = &ast.AlterTableCmd{ColumnName: yyDollar[3].str, ColumnType: yyDollar[5].str}
		}
	case 31:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.node = &ast.AlterDataSourceStmt{DataSourceName: yyDollar[4].str, Options: yyDollar[5].optlist}
		}
	case 32:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.node = &ast.DropDataSourceStmt{DataSourceName: yyDollar[4].str}
		}
	case 33:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.optlist = yyDollar[3].optlist
		}
	case 34:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.optlist = yyDollar[3].optlist
		}
	case 35:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.optlist = yyDollar[1].optlist
		}
	case 36:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.optlist = append(yyDollar[1].optlist, yyDollar[3].optlist...)
		}
	case 37:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.optlist = yyDollar[1].optlist
		}
	case 38:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.optlist = append(yyDollar[1].optlist, yyDollar[3].optlist...)
		}
	case 39:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.optlist = []ast.Option{ast.Option{Name: yyDollar[1].str, Val: yyDollar[2].str}}
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.optlist = []ast.Option{ast.Option{Action: "DROP", Name: yyDollar[2].str, Val: ""}}
		}
	case 41:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.optlist = []ast.Option{ast.Option{Action: "SET", Name: yyDollar[2].str, Val: yyDollar[3].str}}
		}
	case 42:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.optlist = []ast.Option{ast.Option{Action: "ADD", Name: yyDollar[2].str, Val: yyDollar[3].str}}
		}
	case 43:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.optlist = []ast.Option{ast.Option{Action: "ADD", Name: yyDollar[1].str, Val: yyDollar[2].str}}
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.str = yyDollar[1].str
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.str = yyDollar[1].str
		}
	case 46:
		yyDollar = yyS[yypt-12 : yypt+1]
		{
			yyVAL.node = &ast.AuthorizeStmt{DataSourceName: yyDollar[9].str, RoleName: yyDollar[11].str}
		}
	case 47:
		yyDollar = yyS[yypt-12 : yypt+1]
		{
			yyVAL.node = &ast.DeauthorizeStmt{DataSourceName: yyDollar[9].str, RoleName: yyDollar[11].str}
		}
	case 48:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.node = &ast.ListStmt{Name: yyDollar[2].str}
		}
	case 49:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.node = &ast.RefreshInferredColumnTypesStmt{}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.node = &ast.VerifyConsistencyStmt{}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.str = yyDollar[1].str
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.str = yyDollar[1].str
		}
	}
	goto yystack /* stack new state and value */
}
