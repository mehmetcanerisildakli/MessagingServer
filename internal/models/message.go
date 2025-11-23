package models

type MessageType string

const (
	MsgTypeStatus  MessageType = "status"
	MsgTypeControl MessageType = "control"
	MsgTypePublic  MessageType = "public"
	MsgTypePrivate MessageType = "private"
)

type ClientMessage struct {
	Type    MessageType `json:"type"`
	Sender  string      `json:"sender"`
	Target  string      `json:"target, omitempty"`
	Content string      `json:"content"`
}
