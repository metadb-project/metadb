package acl

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

type ObjectType string

const (
	Table    ObjectType = "t"
	Function ObjectType = "f"
)

type Privilege string

const (
	Access Privilege = "a"
)

type ACLItem struct {
	SchemaName string
	ObjectName string
	ObjectType ObjectType
	Privilege  Privilege
	UserName   string
}

// Grant defines database privileges.
func Grant(dq dbx.Queryable, acls []ACLItem) error {
	for i := range acls {
		if acls[i].SchemaName == "" {
			return fmt.Errorf("schema name not specified")
		}
		if acls[i].ObjectName == "" {
			return fmt.Errorf("object name not specified")
		}
		if acls[i].ObjectType != Table && acls[i].ObjectType != Function {
			return fmt.Errorf("unknown object type %v", acls[i].ObjectType)
		}
		if acls[i].Privilege != Access {
			return fmt.Errorf("unknown privilege %v", acls[i].Privilege)
		}
		if acls[i].UserName == "" {
			return fmt.Errorf("user name not specified")
		}
	}

	batch := pgx.Batch{}
	for i := range acls {
		switch acls[i].ObjectType {
		case Table:
			switch acls[i].Privilege {
			case Access:
				batch.Queue("GRANT USAGE ON SCHEMA " + acls[i].SchemaName + " TO " + acls[i].UserName)
				batch.Queue("GRANT SELECT ON " + acls[i].SchemaName + "." + acls[i].ObjectName + " TO " + acls[i].UserName)
			}
		case Function:
			switch acls[i].Privilege {
			case Access:
				batch.Queue("GRANT USAGE ON SCHEMA " + acls[i].SchemaName + " TO " + acls[i].UserName)
				batch.Queue("GRANT EXECUTE ON FUNCTION " + acls[i].SchemaName + "." + acls[i].ObjectName + " TO " + acls[i].UserName)
			}
		}
		batch.Queue("INSERT INTO metadb.acl(schema_name,object_name,object_type,privilege,user_name)VALUES($1,$2,$3,$4,$5)ON CONFLICT DO NOTHING",
			acls[i].SchemaName, acls[i].ObjectName, acls[i].ObjectType, "a", acls[i].UserName)
	}
	if err := dq.SendBatch(context.TODO(), &batch).Close(); err != nil {
		return fmt.Errorf("writing acl: %w", util.PGErr(err))
	}
	return nil
}

// Revoke removes a database privilege.
func Revoke(dq dbx.Queryable, acls []ACLItem) error {
	for i := range acls {
		if acls[i].SchemaName == "" {
			return fmt.Errorf("schema name not specified")
		}
		if acls[i].ObjectName == "" {
			return fmt.Errorf("object name not specified")
		}
		if acls[i].ObjectType != Table && acls[i].ObjectType != Function {
			return fmt.Errorf("unknown object type %v", acls[i].ObjectType)
		}
		if acls[i].Privilege != Access {
			return fmt.Errorf("unknown privilege %v", acls[i].Privilege)
		}
		if acls[i].UserName == "" {
			return fmt.Errorf("user name not specified")
		}
	}

	batch := pgx.Batch{}
	for i := range acls {
		switch acls[i].ObjectType {
		case Table:
			switch acls[i].Privilege {
			case Access:
				batch.Queue("REVOKE SELECT ON " + acls[i].SchemaName + "." + acls[i].ObjectName + " FROM " + acls[i].UserName)
			}
		case Function:
			switch acls[i].Privilege {
			case Access:
				batch.Queue("REVOKE EXECUTE ON FUNCTION " + acls[i].SchemaName + "." + acls[i].ObjectName + " FROM " + acls[i].UserName)
			}
		}
		batch.Queue("DELETE FROM metadb.acl WHERE schema_name=$1 AND object_name=$2 AND object_type=$3 AND privilege=$4 AND user_name=$5",
			acls[i].SchemaName, acls[i].ObjectName, acls[i].ObjectType, acls[i].Privilege, acls[i].UserName)
	}
	if err := dq.SendBatch(context.TODO(), &batch).Close(); err != nil {
		return fmt.Errorf("writing acl: %w", util.PGErr(err))
	}
	return nil
}

func RevokeAllFromUser(dq dbx.Queryable, userName string) error {
	acls, err := readPrivilegesOfUser(dq, userName)
	if err != nil {
		return err
	}
	if err := Revoke(dq, acls); err != nil {
		return err
	}
	return nil
}

func RevokeAllOnObject(dq dbx.Queryable, schemaName, objectName string, objectType ObjectType) error {
	q := "DELETE FROM metadb.acl WHERE schema_name=$1 AND object_name=$2 AND object_type=$3"
	if _, err := dq.Exec(context.TODO(), q, schemaName, objectName, objectType); err != nil {
		return util.PGErr(err)
	}
	return nil
}

// RestorePrivileges reapplies all privileges that were previously defined using Grant().
// It is intended for restoring privileges after an object has been recreated.
func RestorePrivileges(dq dbx.Queryable, schemaName, objectName string, objectType ObjectType) error {
	if schemaName == "" {
		return fmt.Errorf("schema name not specified")
	}
	if objectName == "" {
		return fmt.Errorf("object name not specified")
	}
	if objectType != Table && objectType != Function {
		return fmt.Errorf("unknown object type %v", objectType)
	}

	acls, err := readPrivileges(dq, schemaName, objectName, objectType)
	if err != nil {
		return fmt.Errorf("reading acl: %w", util.PGErr(err))
	}

	if err = Grant(dq, acls); err != nil {
		return err
	}

	return nil
}

func readPrivileges(dq dbx.Queryable, schemaName, objectName string, objectType ObjectType) ([]ACLItem, error) {
	rows, err := dq.Query(context.TODO(),
		"SELECT privilege, user_name FROM metadb.acl WHERE schema_name=$1 AND object_name=$2 AND object_type=$3",
		schemaName, objectName, objectType)
	if err != nil {
		return nil, fmt.Errorf("selecting acl: %w", util.PGErr(err))
	}
	defer rows.Close()
	acls := make([]ACLItem, 0)
	for rows.Next() {
		var p, u string
		err = rows.Scan(&p, &u)
		if err != nil {
			return nil, fmt.Errorf("reading acl: %w", util.PGErr(err))
		}
		acls = append(acls, ACLItem{
			SchemaName: schemaName,
			ObjectName: objectName,
			ObjectType: objectType,
			Privilege:  Privilege(p),
			UserName:   u,
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("reading acl: %w", util.PGErr(err))
	}
	return acls, nil
}

func readPrivilegesOfUser(dq dbx.Queryable, userName string) ([]ACLItem, error) {
	rows, err := dq.Query(context.TODO(),
		"SELECT schema_name, object_name, object_type, privilege FROM metadb.acl WHERE user_name=$1",
		userName)
	if err != nil {
		return nil, fmt.Errorf("selecting acl: %w", util.PGErr(err))
	}
	defer rows.Close()
	acls := make([]ACLItem, 0)
	for rows.Next() {
		var s, o, t, p string
		err = rows.Scan(&s, &o, &t, &p)
		if err != nil {
			return nil, fmt.Errorf("reading acl: %w", util.PGErr(err))
		}
		acls = append(acls, ACLItem{
			SchemaName: s,
			ObjectName: o,
			ObjectType: ObjectType(t),
			Privilege:  Privilege(p),
			UserName:   userName,
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("reading acl: %w", util.PGErr(err))
	}
	return acls, nil
}
