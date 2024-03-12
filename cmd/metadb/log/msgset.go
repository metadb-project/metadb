package log

type MessageSet struct {
	Messages map[string]struct{}
}

func NewMessageSet() *MessageSet {
	return &MessageSet{
		Messages: make(map[string]struct{}),
	}
}

// Insert adds a message to the set and returns true if the message
// was added, false if the set already contained the message.
func (d *MessageSet) Insert(msg string) bool {
	_, ok := d.Messages[msg]
	if ok {
		return false
	}
	d.Messages[msg] = struct{}{}
	return true
}
