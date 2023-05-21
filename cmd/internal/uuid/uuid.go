package uuid

import (
	"regexp"

	"github.com/jackc/pgx/v5/pgtype"
)

func IsUUID(str string) bool {
	return uuidRegexp.MatchString(str)
}

func EncodeUUID(uuid string) (pgtype.UUID, error) {
	var u pgtype.UUID
	err := u.Scan(uuid)
	if err != nil {
		return pgtype.UUID{}, err
	}
	return u, nil
}

func EncodeNilUUID() pgtype.UUID {
	u, err := EncodeUUID(NilUUID)
	if err != nil {
		panic("error encoding nil UUID")
	}
	return u
}

var uuidRegexp = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`)

const NilUUID string = "00000000-0000-0000-0000-000000000000"
