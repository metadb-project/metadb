package log

import "sync"

type MessageSet struct {
	mu       sync.Mutex
	messages map[string]struct{}
}

func NewMessageSet() *MessageSet {
	return &MessageSet{
		messages: make(map[string]struct{}),
	}
}

// Insert adds a message to the set and returns true if the message
// was added, false if the set already contained the message.
func (d *MessageSet) Insert(msg string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, ok := d.messages[msg]
	if ok {
		return false
	}
	d.messages[msg] = struct{}{}
	return true
}
