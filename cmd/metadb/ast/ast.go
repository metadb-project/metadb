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

type CreateDataOriginStmt struct {
	OriginName string
}

func (*CreateDataOriginStmt) node()     {}
func (*CreateDataOriginStmt) stmtNode() {}

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

type CreateUserStmt struct {
	UserName string
	Options  []Option
}

func (*CreateUserStmt) node()     {}
func (*CreateUserStmt) stmtNode() {}

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
