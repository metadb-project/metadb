%{
package parser

import (
	"github.com/metadb-project/metadb/cmd/metadb/ast"
)

%}

%union{
	str string
	funcparamtypelist []string
	optlist []ast.Option
	node ast.Node
	pass bool
}

%type <node> deregister_user_stmt
%type <node> register_user_stmt
%type <node> top_level_stmt stmt
%type <node> select_stmt
%type <node> grant_stmt revoke_stmt
%type <node> create_data_source_stmt alter_data_source_stmt drop_data_source_stmt authorize_stmt deauthorize_stmt
%type <node> create_user_stmt drop_user_stmt
%type <node> create_data_mapping_stmt create_data_origin_stmt list_stmt
%type <node> refresh_inferred_column_types_stmt
%type <node> alter_table_stmt alter_table_cmd
%type <node> verify_consistency_stmt
%type <node> create_schema_for_user_stmt
%type <funcparamtypelist> parameter_type
%type <funcparamtypelist> parameter_type_list
%type <optlist> options_clause alter_options_clause option_list alter_option_list option alter_option
%type <str> option_name option_val
%type <str> name unreserved_keyword
/*
%type <str> boolean
*/

%token DEREGISTER
%token FUNCTION
%token REGISTER
%token SELECT
%token TABLE
%token CONSISTENCY
%token CREATE GRANT REVOKE ACCESS ALTER DATA SOURCE ORIGIN OPTIONS USER
%token AUTHORIZE DEAUTHORIZE ON ALL TABLES IN TO WITH MAPPING LIST
%token REFRESH INFERRED COLUMN TYPES
%token TYPE
%token TRUE FALSE
%token VERIFY FOR FROM PATH
%token SCHEMA
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
	| create_data_mapping_stmt
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
	| drop_user_stmt
		{
			$$ = $1
		}
	| create_schema_for_user_stmt
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
	| alter_table_stmt
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
	| deauthorize_stmt
		{
			$$ = $1
		}
	| register_user_stmt
		{
			$$ = $1
		}
	| deregister_user_stmt
		{
			$$ = $1
		}
	| grant_stmt
		{
			$$ = $1
		}
	| revoke_stmt
		{
			$$ = $1
		}
	| list_stmt
		{
			$$ = $1
		}
	| refresh_inferred_column_types_stmt
		{
			$$ = $1
		}
	| verify_consistency_stmt
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

create_data_mapping_stmt:
	CREATE DATA MAPPING FOR name FROM TABLE name COLUMN name PATH SLITERAL TO SLITERAL ';'
		{
			$$ = &ast.CreateDataMappingStmt{TypeName: $5, TableName: $8, ColumnName: $10, Path: $12, TargetIdentifier: $14}
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
	| CREATE USER MAPPING
		{
			yylex.(*lexer).pass = true
		}

grant_stmt:
	GRANT ACCESS ON ALL TO name ';'
		{
			$$ = &ast.GrantAccessOnAllStmt{UserName: $6}
		}
	| GRANT ACCESS ON TABLE name TO name ';'
		{
			$$ = &ast.GrantAccessOnTableStmt{TableName: $5, UserName: $7}
		}
	| GRANT ACCESS ON FUNCTION name '(' ')' TO name ';'
		{
			$$ = &ast.GrantAccessOnFunctionStmt{FunctionName: $5, UserName: $9}
		}
	| GRANT ACCESS ON FUNCTION name '(' parameter_type_list ')' TO name ';'
		{
			$$ = &ast.GrantAccessOnFunctionStmt{FunctionName: $5, FunctionParameterTypes: $7, UserName: $10}
		}

parameter_type_list:
	parameter_type
		{
			$$ = $1
		}
	| parameter_type_list ',' parameter_type
		{
			$$ = append($1, $3...)
		}

parameter_type:
	name
		{
			$$ = []string{$1}
		}

revoke_stmt:
	REVOKE ACCESS ON ALL FROM name ';'
		{
			$$ = &ast.RevokeAccessOnAllStmt{UserName: $6}
		}
	| REVOKE ACCESS ON TABLE name FROM name ';'
		{
			$$ = &ast.RevokeAccessOnTableStmt{TableName: $5, UserName: $7}
		}
	| REVOKE ACCESS ON FUNCTION name '(' ')' FROM name ';'
		{
			$$ = &ast.RevokeAccessOnFunctionStmt{FunctionName: $5, UserName: $9}
		}
	| REVOKE ACCESS ON FUNCTION name '(' parameter_type_list ')' FROM name ';'
		{
			$$ = &ast.RevokeAccessOnFunctionStmt{FunctionName: $5, FunctionParameterTypes: $7, UserName: $10}
		}

deregister_user_stmt:
	DEREGISTER USER name ';'
		{
			$$ = &ast.DeregisterUserStmt{UserName: $3}
		}

register_user_stmt:
	REGISTER USER name ';'
		{
			$$ = &ast.RegisterUserStmt{UserName: $3}
		}

drop_user_stmt:
	DROP USER name ';'
		{
			$$ = &ast.DropUserStmt{UserName: $3}
		}

create_schema_for_user_stmt:
	CREATE SCHEMA FOR USER name ';'
		{
			$$ = &ast.CreateSchemaForUserStmt{UserName: $5}
		}

alter_table_stmt:
	ALTER TABLE name alter_table_cmd ';'
		{
			$$ = &ast.AlterTableStmt{TableName: $3, Cmd: ($4).(*ast.AlterTableCmd)}
		}

alter_table_cmd:
	ALTER COLUMN name TYPE name
		{
			$$ = &ast.AlterTableCmd{ColumnName: $3, ColumnType: $5}
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

deauthorize_stmt:
    DEAUTHORIZE SELECT ON ALL TABLES IN DATA SOURCE name FROM name ';'
		{
			$$ = &ast.DeauthorizeStmt{DataSourceName: $9, RoleName: $11}
		}

list_stmt:
    LIST name ';'
		{
			$$ = &ast.ListStmt{Name: $2}
		}

refresh_inferred_column_types_stmt:
    REFRESH INFERRED COLUMN TYPES ';'
		{
			$$ = &ast.RefreshInferredColumnTypesStmt{}
		}

verify_consistency_stmt:
    VERIFY CONSISTENCY ';'
		{
			$$ = &ast.VerifyConsistencyStmt{}
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
