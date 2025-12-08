package server

import (
	"MessagingServer/internal/models"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

type Hub struct {
	clients    map[string]*Client
	Broadcast  chan []byte
	Register   chan *Client
	unregister chan *Client
	mutex      sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[string]*Client),
		Broadcast:  make(chan []byte),
		Register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.mutex.Lock()
			log.Println("Register client:", client)
			h.clients[client.User.ID] = client
			h.mutex.Unlock()
		case client := <-h.unregister:
			h.mutex.Lock()
			if _, ok := h.clients[client.User.ID]; ok {
				leaveMsg := models.ClientMessage{
					Type:       models.MsgTypeStatus,
					Content:    fmt.Sprintf(`{"%s leaved the chat"}`, client.User.Username),
					Sender:     client.User.ID,
					SenderName: client.User.Username,
					Timestamp:  time.Now(),
				}
				leaveBytes, _ := json.Marshal(leaveMsg)

				delete(h.clients, client.User.ID)
				close(client.Send)
				h.mutex.Unlock()
				h.Broadcast <- leaveBytes
			} else {
				h.mutex.Unlock()
			}
			log.Println("Unregister client:", client)
		case rawMessage := <-h.Broadcast:
			var msg models.ClientMessage
			if err := json.Unmarshal(rawMessage, &msg); err != nil {
				log.Println("message read error: ", err)
				continue
			}
			if msg.Sender == "" {
				continue
			}
			switch msg.Type {
			case models.MsgTypePublic:
				h.BroadcastPublic(rawMessage)
			case models.MsgTypePrivate:
				h.BroadcastPrivate(rawMessage, msg.Target)
			case models.MsgTypeStatus, models.MsgTypeControl:
				h.BroadcastPublic(rawMessage)
			default:
				log.Println("Unknown message type:", msg.Type)
			}
		}
	}
}

func (h *Hub) BroadcastPublic(rawMessage []byte) {
	h.mutex.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for _, client := range h.clients {
		if client.User.IsActive {
			clients = append(clients, client)
		}
	}
	h.mutex.RUnlock()

	for _, client := range clients {
		select {
		case client.Send <- rawMessage:
		default:
			h.mutex.Lock()
			if _, ok := h.clients[client.User.ID]; ok {
				close(client.Send)
				delete(h.clients, client.User.ID)
			}
			h.mutex.Unlock()
		}
	}
}

func (h *Hub) BroadcastPrivate(rawMessage []byte, targetID string) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	var msg models.ClientMessage
	if err := json.Unmarshal(rawMessage, &msg); err != nil {
		log.Println("message read error: ", err)
		return
	}
	h.mutex.RLock()
	targetClient, targetOk := h.clients[targetID]
	senderClient, senderOk := h.clients[msg.Sender]
	h.mutex.RUnlock()
	if targetOk {
		if targetClient.User.IsActive {
			select {
			case targetClient.Send <- rawMessage:
			default:
				log.Println("can not send message to client (channel is full):", targetClient)
				h.mutex.Lock()
				if _, ok := h.clients[targetID]; ok {
					close(targetClient.Send)
					delete(h.clients, targetID)
				}
				h.mutex.Unlock()
			}
		} else {
			log.Println("can not find client (target user is passive):", targetID)
		}
	} else {
		log.Println("can not find client (target user is not exist):", targetID)
	}

	if msg.Sender != "" && senderOk {
		if senderClient.User.IsActive {
			select {
			case senderClient.Send <- rawMessage:
			default:
			}
		}
	}

}

func (h *Hub) ToggleUserActiveStatus(targetUserID string, isActive bool) bool {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if client, ok := h.clients[targetUserID]; ok {
		client.User.IsActive = isActive
		log.Println("Toggle user active status:", isActive)
		return true
	}
	return false
}

func (h *Hub) GetAllUsers() []*models.User {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	users := make([]*models.User, 0, len(h.clients))
	for _, client := range h.clients {
		users = append(users, client.User)
	}
	return users
}

func (h *Hub) GetUser(userID string) *models.User {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	if client, ok := h.clients[userID]; ok {
		return client.User
	}
	return nil
}
