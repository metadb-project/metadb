package ast

type Option struct {
	Action string
	Name   string
	Val    string
}

type Node interface {
	node()
}

type Stmt interface {
	Node
	stmtNode()
}

type SelectStmt struct {
}

func (*SelectStmt) node()     {}
func (*SelectStmt) stmtNode() {}

type CreateDataSourceStmt struct {
	DataSourceName string
	TypeName       string
	Options        []Option
}

func (*CreateDataSourceStmt) node()     {}
func (*CreateDataSourceStmt) stmtNode() {}

type CreateDataMappingStmt struct {
	TypeName         string
	TableName        string
	ColumnName       string
	Path             string
	TargetIdentifier string
}

func (*CreateDataMappingStmt) node()     {}
func (*CreateDataMappingStmt) stmtNode() {}

type CreateDataOriginStmt struct {
	OriginName string
}

func (*CreateDataOriginStmt) node()     {}
func (*CreateDataOriginStmt) stmtNode() {}

type DropDataMappingStmt struct {
	TypeName   string
	TableName  string
	ColumnName string
	Path       string
}

func (*DropDataMappingStmt) node()     {}
func (*DropDataMappingStmt) stmtNode() {}

type AlterDataSourceStmt struct {
	DataSourceName string
	Options        []Option
}

func (*AlterDataSourceStmt) node()     {}
func (*AlterDataSourceStmt) stmtNode() {}

type DropDataSourceStmt struct {
	DataSourceName string
}

func (*DropDataSourceStmt) node()     {}
func (*DropDataSourceStmt) stmtNode() {}

type AuthorizeStmt struct {
	DataSourceName string
	RoleName       string
}

func (*AuthorizeStmt) node()     {}
func (*AuthorizeStmt) stmtNode() {}

type DeauthorizeStmt struct {
	DataSourceName string
	RoleName       string
}

func (*DeauthorizeStmt) node()     {}
func (*DeauthorizeStmt) stmtNode() {}

type CreateUserStmt struct {
	UserName string
	Options  []Option
}

func (*CreateUserStmt) node()     {}
func (*CreateUserStmt) stmtNode() {}

type DropUserStmt struct {
	UserName string
}

func (*DropUserStmt) node()     {}
func (*DropUserStmt) stmtNode() {}

type ListStmt struct {
	Name string
}

func (*ListStmt) node()     {}
func (*ListStmt) stmtNode() {}

type RefreshInferredColumnTypesStmt struct {
}

func (*RefreshInferredColumnTypesStmt) node()     {}
func (*RefreshInferredColumnTypesStmt) stmtNode() {}

type AlterTableStmt struct {
	TableName string
	Cmd       *AlterTableCmd
}

func (*AlterTableStmt) node()     {}
func (*AlterTableStmt) stmtNode() {}

type AlterTableCmd struct {
	ColumnName string
	ColumnType string
}

func (*AlterTableCmd) node() {}

type VerifyConsistencyStmt struct {
}

func (*VerifyConsistencyStmt) node()     {}
func (*VerifyConsistencyStmt) stmtNode() {}

type CreateSchemaForUserStmt struct {
	UserName string
}

func (*CreateSchemaForUserStmt) node()     {}
func (*CreateSchemaForUserStmt) stmtNode() {}

type GrantAccessOnAllStmt struct {
	UserName string
}

func (*GrantAccessOnAllStmt) node()     {}
func (*GrantAccessOnAllStmt) stmtNode() {}

type GrantAccessOnTableStmt struct {
	TableName string
	UserName  string
}

func (*GrantAccessOnTableStmt) node()     {}
func (*GrantAccessOnTableStmt) stmtNode() {}

type GrantAccessOnFunctionStmt struct {
	FunctionName           string
	FunctionParameterTypes []string
	UserName               string
}

func (*GrantAccessOnFunctionStmt) node()     {}
func (*GrantAccessOnFunctionStmt) stmtNode() {}

type PurgeDataDropTableStmt struct {
	TableName string
}

func (*PurgeDataDropTableStmt) node()     {}
func (*PurgeDataDropTableStmt) stmtNode() {}

type RevokeAccessOnAllStmt struct {
	UserName string
}

func (*RevokeAccessOnAllStmt) node()     {}
func (*RevokeAccessOnAllStmt) stmtNode() {}

type RevokeAccessOnTableStmt struct {
	TableName string
	UserName  string
}

func (*RevokeAccessOnTableStmt) node()     {}
func (*RevokeAccessOnTableStmt) stmtNode() {}

type RevokeAccessOnFunctionStmt struct {
	FunctionName           string
	FunctionParameterTypes []string
	UserName               string
}

func (*RevokeAccessOnFunctionStmt) node()     {}
func (*RevokeAccessOnFunctionStmt) stmtNode() {}

type DeregisterUserStmt struct {
	UserName string
}

func (*DeregisterUserStmt) node()     {}
func (*DeregisterUserStmt) stmtNode() {}

type RegisterUserStmt struct {
	UserName string
}

func (*RegisterUserStmt) node()     {}
func (*RegisterUserStmt) stmtNode() {}

type AlterSystemStmt struct {
	ConfigParameter string
	Value           string
}

func (*AlterSystemStmt) node()     {}
func (*AlterSystemStmt) stmtNode() {}
