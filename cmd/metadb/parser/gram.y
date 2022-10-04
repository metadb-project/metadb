%{
package parser

import (
	"github.com/metadb-project/metadb/cmd/metadb/ast"
)

%}

%union{
	str string
	optlist []ast.Option
	node ast.Node
	pass bool
}

%type <node> top_level_stmt stmt
%type <node> select_stmt
%type <node> create_server_stmt
%type <optlist> options_clause option_list option
%type <str> option_name option_val
%type <str> name unreserved_keyword
%type <str> boolean

%token SELECT
%token CREATE SERVER IF DATA SOURCE OPTIONS
%token TYPE FOREIGN
%token TRUE FALSE
%token <str> VERSION
%token <str> IDENT NUMBER
%token <str> SLITERAL

%start main

%%

main:
	top_level_stmt
		{
			yylex.(*lexer).node = $1
		}

top_level_stmt:
	stmt
		{
			$$ = $1
		}

stmt:
	select_stmt
		{
			$$ = $1
		}
	| create_server_stmt
		{
			$$ = $1
		}
	| CREATE
		{
			yylex.(*lexer).pass = true
			// $$ = nil
		}
	| IDENT
		{
			yylex.(*lexer).pass = true
			// $$ = nil
		}

select_stmt:
	SELECT VERSION '(' ')' ';'
		{
			$$ = &ast.SelectStmt{Fn: $2}
		}
	| SELECT
		{
			yylex.(*lexer).pass = true
			// $$ = &ast.SelectStmt{}
		}

create_server_stmt:
	CREATE SERVER name DATA SOURCE name options_clause ';'
		{
			$$ = &ast.CreateServerStmt{ServerName: $3, TypeName: $6, Options: $7}
		}
	| CREATE SERVER name
		{
			yylex.(*lexer).pass = true
			// $$ = &ast.CreateServerStmt{}
		}
	| CREATE SERVER IF
		{
			yylex.(*lexer).pass = true
			// $$ = &ast.CreateServerStmt{}
		}

options_clause:
	OPTIONS '(' option_list ')'
		{
			$$ = $3
		}

option_list:
	option
		{
			$$ = $1
		}
	| option_list ',' option
		{
			$$ = append($1, $3...)
		}

option:
	option_name option_val
		{
			$$ = []ast.Option{ast.Option{Name: $1, Val: $2}}
		}

option_name:
	name
		{
			$$ = $1
		}

option_val:
	SLITERAL
		{
			$$ = $1
		}
	| boolean
		{
			$$ = $1
		}

name:
	IDENT
		{
			$$ = $1
		}
	| unreserved_keyword
		{
			$$ = $1
		}

boolean:
	TRUE
		{
			$$ = "true"
		}
	| FALSE
		{
			$$ = "false"
		}

unreserved_keyword:
	VERSION
