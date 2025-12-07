package models

import "time"

type MessageType string

const (
	MsgTypeStatus  MessageType = "status"
	MsgTypeControl MessageType = "control"
	MsgTypePublic  MessageType = "public"
	MsgTypePrivate MessageType = "private"
)

type ClientMessage struct {
	Type       MessageType `json:"type"`
	Sender     string      `json:"sender"`
	SenderName string      `json:"sender_name,omitempty"`
	Target     string      `json:"target,omitempty"`
	Content    string      `json:"content"`
	Timestamp  time.Time   `json:"timestamp"`
}
