package util

import "os"

// TODO duplicate
// ModePermRW is the umask "-rw-------".
const ModePermRW = 0600

// TODO duplicate
// ModePermRWX is the umask "-rwx------".
const ModePermRWX = 0700

// TODO duplicate
// FileExists returns true if f is an existing file or directory.
func FileExists(f string) (bool, error) {
	_, err := os.Stat(f)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

//func CloseRows(rows *sql.Rows) {
//        _ = rows.Close()
//}

//func Rollback(tx *sql.Tx) {
//        _ = tx.Rollback()
//}
