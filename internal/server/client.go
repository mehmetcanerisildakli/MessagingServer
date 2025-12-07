package server

import (
	"MessagingServer/internal/models"
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	User       *models.User
	Hub        *Hub
	Connection *websocket.Conn
	Send       chan []byte
}

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

func NewClient(user *models.User, hub *Hub, connection *websocket.Conn) *Client {
	return &Client{
		User:       user,
		Hub:        hub,
		Connection: connection,
		Send:       make(chan []byte, maxMessageSize),
	}
}

func (c *Client) ReadPump() {
	defer func() {
		c.Hub.unregister <- c
		err := c.Connection.Close()
		if err != nil {
			return
		}
	}()
	c.Connection.SetReadLimit(maxMessageSize)
	err := c.Connection.SetReadDeadline(time.Now().Add(pongWait))
	if err != nil {
		return
	}
	c.Connection.SetPongHandler(func(string) error {
		return c.Connection.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, message, err := c.Connection.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf(" reading error: %v", err)
			}
			break
		}
		var clientMsg models.ClientMessage
		if err = json.Unmarshal(message, &clientMsg); err != nil {
			log.Println("Error unmarshalling client message:", err)
			continue
		}
		clientMsg.Sender = c.User.ID
		clientMsg.SenderName = c.User.Username
		clientMsg.Timestamp = time.Now()
		messageBytes, err := json.Marshal(clientMsg)
		if err != nil {
			log.Println("Error marshalling client message:", err)
			continue
		}
		c.Hub.Broadcast <- messageBytes
	}
}

func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		err := c.Connection.Close()
		if err != nil {
			return
		}
	}()
	for {
		select {
		case message, ok := <-c.Send:
			c.Connection.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.Connection.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := c.Connection.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, err = w.Write(message)
			if err != nil {
				return
			}
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.Connection.SetWriteDeadline(time.Now().Add(writeWait))
			err := c.Connection.WriteMessage(websocket.PingMessage, nil)
			if err != nil {
				return
			}
		}
	}
}
