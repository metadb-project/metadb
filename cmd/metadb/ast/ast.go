package ast

type Option struct {
	Name string
	Val  string
}

type Node interface {
	node()
}

type Stmt interface {
	Node
	stmtNode()
}

type SelectStmt struct {
	Fn string
}

func (*SelectStmt) node()     {}
func (*SelectStmt) stmtNode() {}

type CreateServerStmt struct {
	ServerName string
	TypeName   string
	Options    []Option
}

func (*CreateServerStmt) node()     {}
func (*CreateServerStmt) stmtNode() {}
