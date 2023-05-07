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
%type <node> create_data_source_stmt alter_data_source_stmt drop_data_source_stmt authorize_stmt create_user_stmt
%type <node> create_data_origin_stmt list_stmt
%type <optlist> options_clause alter_options_clause option_list alter_option_list option alter_option
%type <str> option_name option_val
%type <str> name unreserved_keyword
/*
%type <str> boolean
*/

%token SELECT
%token CREATE ALTER DATA SOURCE ORIGIN OPTIONS USER
%token AUTHORIZE ON ALL TABLES IN TO WITH MAPPING LIST
%token TYPE
%token TRUE FALSE
%token <str> VERSION
%token <str> ADD SET DROP
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
	| create_data_source_stmt
		{
			$$ = $1
		}
	| create_data_origin_stmt
		{
			$$ = $1
		}
	| create_user_stmt
		{
			$$ = $1
		}
	| CREATE
		{
			yylex.(*lexer).pass = true
			// $$ = nil
		}
	| alter_data_source_stmt
		{
			$$ = $1
		}
	| ALTER
		{
			yylex.(*lexer).pass = true
			// $$ = nil
		}
	| drop_data_source_stmt
		{
			$$ = $1
		}
	| DROP
		{
			yylex.(*lexer).pass = true
			// $$ = nil
		}
	| authorize_stmt
		{
			$$ = $1
		}
	| list_stmt
		{
			$$ = $1
		}
	| SET
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
	SELECT
		{
			yylex.(*lexer).pass = true
			$$ = &ast.SelectStmt{}
		}

create_data_source_stmt:
	CREATE DATA SOURCE name TYPE name options_clause ';'
		{
			$$ = &ast.CreateDataSourceStmt{DataSourceName: $4, TypeName: $6, Options: $7}
		}

create_data_origin_stmt:
	CREATE DATA ORIGIN name ';'
		{
			$$ = &ast.CreateDataOriginStmt{OriginName: $4}
		}

create_user_stmt:
	CREATE USER name WITH option_list ';'
		{
			$$ = &ast.CreateUserStmt{UserName: $3, Options: $5}
		}
	| CREATE USER name option_list ';'
		{
			$$ = &ast.CreateUserStmt{UserName: $3, Options: $4}
		}
	| CREATE USER MAPPING
		{
			yylex.(*lexer).pass = true
		}

alter_data_source_stmt:
	ALTER DATA SOURCE name alter_options_clause ';'
		{
			$$ = &ast.AlterDataSourceStmt{DataSourceName: $4, Options: $5}
		}

drop_data_source_stmt:
	DROP DATA SOURCE name ';'
		{
			$$ = &ast.DropDataSourceStmt{DataSourceName: $4}
		}

options_clause:
     OPTIONS '(' option_list ')'
		{
			$$ = $3
		}

alter_options_clause:
     OPTIONS '(' alter_option_list ')'
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

alter_option_list:
	alter_option
		{
			$$ = $1
		}
	| alter_option_list ',' alter_option
		{
			$$ = append($1, $3...)
		}

option:
	option_name option_val
		{
			$$ = []ast.Option{ast.Option{Name: $1, Val: $2}}
		}

alter_option:
	DROP option_name
		{
			$$ = []ast.Option{ast.Option{Action: "DROP", Name: $2, Val: ""}}
		}
	| SET option_name option_val
		{
			$$ = []ast.Option{ast.Option{Action: "SET", Name: $2, Val: $3}}
		}
	| ADD option_name option_val
		{
			$$ = []ast.Option{ast.Option{Action: "ADD", Name: $2, Val: $3}}
		}
	| option_name option_val
		{
			$$ = []ast.Option{ast.Option{Action: "ADD", Name: $1, Val: $2}}
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

authorize_stmt:
    AUTHORIZE SELECT ON ALL TABLES IN DATA SOURCE name TO name ';'
		{
			$$ = &ast.AuthorizeStmt{DataSourceName: $9, RoleName: $11}
		}

list_stmt:
    LIST name ';'
		{
			$$ = &ast.ListStmt{Name: $2}
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

/*
boolean:
	TRUE
		{
			$$ = "true"
		}
	| FALSE
		{
			$$ = "false"
		}
*/

unreserved_keyword:
	VERSION
