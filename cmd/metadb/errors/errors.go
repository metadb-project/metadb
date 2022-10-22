package errors

type Error struct {
	Err  error
	Hint string
}

func (e Error) Error() string {
	return e.Err.Error()
}
