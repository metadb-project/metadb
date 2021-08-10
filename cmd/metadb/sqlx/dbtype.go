package sqlx

import (
	"fmt"
)

func NewDBType(typeName string) (DBType, error) {
	switch typeName {
	case "postgresql":
		return &Postgres{}, nil
	case "redshift":
		return &Redshift{}, nil
	default:
		return nil, fmt.Errorf("Unknown database type: %s", typeName)
	}
}

type DBType interface {
	TypeName() string
}
