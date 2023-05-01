package change

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/metadb-project/metadb/cmd/metadb/util"
)

type Event struct {
	Key     *EventKey
	Value   *EventValue
	Message *kafka.Message
}

func NewEvent(msg *kafka.Message) (*Event, error) {
	if msg == nil {
		return nil, fmt.Errorf("creating change event: message is nil")
	}
	var ce = new(Event)
	var err error
	if msg.Key != nil && len(msg.Key) > 0 {
		if err = json.Unmarshal(msg.Key, &(ce.Key)); err != nil {
			return nil, fmt.Errorf("change event key: %s\n%s", err, util.KafkaMessageString(msg))
		}
	}
	if msg.Value != nil && len(msg.Value) > 0 {
		if err = json.Unmarshal(msg.Value, &(ce.Value)); err != nil {
			return nil, fmt.Errorf("change event value: %s\n%s", err, util.KafkaMessageString(msg))
		}
	}
	ce.Message = msg
	return ce, nil
}

func (e Event) String() string {
	var key, value, message string
	if e.Key != nil {
		key = fmt.Sprintf("%v", *e.Key)
	}
	if e.Value != nil {
		value = fmt.Sprintf("%v", *e.Value)
	}
	if e.Message != nil {
		message = util.KafkaMessageString(e.Message)
	}
	return fmt.Sprintf("key = %s\nvalue = %s\nmessage =\n%s", key, value, message)
}

type EventKeySchema struct {
	Type     *string                  `json:"type"`
	Fields   []map[string]interface{} `json:"fields"`
	Optional *bool                    `json:"optional"`
	Name     *string                  `json:"name"`
}

func (e EventKeySchema) String() string {
	var tp, fields, optional, name string
	if e.Type != nil {
		tp = fmt.Sprintf("%q", *e.Type)
	}
	if e.Fields != nil {
		fields = fmt.Sprintf("%v", e.Fields)
	}
	if e.Optional != nil {
		optional = fmt.Sprintf("%t", *e.Optional)
	}
	if e.Name != nil {
		name = fmt.Sprintf("%q", *e.Name)
	}
	return fmt.Sprintf("type=%s fields=%s optional=%s name=%s", tp, fields, optional, name)
}

type EventKey struct {
	Schema  *EventKeySchema        `json:"schema"`
	Payload map[string]interface{} `json:"payload"`
}

func (e EventKey) String() string {
	var schema, payload string
	if e.Schema != nil {
		schema = fmt.Sprintf("%v", *e.Schema)
	}
	if e.Payload != nil {
		payload = fmt.Sprintf("%v", e.Payload)
	}
	return fmt.Sprintf("schema={%v} payload={%v}", schema, payload)
}

type EventPayloadSource struct {
	Version   *string  `json:"version"`
	Connector *string  `json:"connector"`
	Name      *string  `json:"name"`
	TsMs      *float64 `json:"ts_ms"`
	Snapshot  *string  `json:"snapshot"`
	DB        *string  `json:"db"`
	Schema    *string  `json:"schema"`
	Table     *string  `json:"table"`
}

type EventValueSchema struct {
	Type     *string                  `json:"type"`
	Fields   []map[string]interface{} `json:"fields"`
	Optional *bool                    `json:"optional"`
	Name     *string                  `json:"name"`
}

type EventValuePayload struct {
	Before      *json.RawMessage       `json:"before"`
	After       map[string]interface{} `json:"after"`
	Source      *EventPayloadSource    `json:"source"`
	Op          *string                `json:"op"`
	TsMs        *int64                 `json:"ts_ms"`
	Transaction *json.RawMessage       `json:"transaction"`
}

type EventValue struct {
	Schema  *EventValueSchema  `json:"schema"`
	Payload *EventValuePayload `json:"payload"`
}
