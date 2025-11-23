package server

import (
	"MessagingServer/internal/models"
	"encoding/json"
	"log"
	"sync"
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
				delete(h.clients, client.User.ID)
				close(client.send)
			}
			log.Println("Unregister client:", client)
			h.mutex.Unlock()
		case message := <-h.Broadcast:
			h.mutex.RLock()
			for _, client := range h.clients {
				if client.User.IsActive {
					select {
					case client.send <- message:
					default:
						close(client.send)
						delete(h.clients, client.User.ID)
					}
				}
				log.Printf("Broadcast message: %s  id: %s\n", string(message), client.User.ID)
			}
			h.mutex.RUnlock()
		case rawMessage := <-h.Broadcast:
			var msg models.ClientMessage
			if err := json.Unmarshal(rawMessage, &msg); err != nil {
				log.Println("message read error")
			}
			switch msg.Type {
			case models.MsgTypePublic:
				h.BroadcastPublic(rawMessage)
			case models.MsgTypePrivate:
				h.BroadcastPrivate(rawMessage, msg.Target)
			default:
				log.Println("unknown message type:", msg.Type)
			}
		}
	}
}

func (h *Hub) BroadcastPublic(rawMessage []byte) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	for _, client := range h.clients {
		if client.User.IsActive {
			select {
			case client.send <- rawMessage:
			default:
				close(client.send)
				delete(h.clients, client.User.ID)
			}
		}
	}
}

func (h *Hub) BroadcastPrivate(rawMessage []byte, targetID string) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	if targetClient, ok := h.clients[targetID]; ok {
		if targetClient.User.IsActive {
			select {
			case targetClient.send <- rawMessage:
			default:
				log.Println("can not send message to client (channel is full):", targetClient)
				close(targetClient.send)
				delete(h.clients, targetClient.User.ID)
			}
		} else {
			log.Println("can not find client (target user is passive):", targetID)
		}
	} else {
		log.Println("can not find client (target user is not exist):", targetID)
	}

}

func (h *Hub) ToggleUserActiveStatus(targetUserID string, isActive bool) bool {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if client, ok := h.clients[targetUserID]; ok {
		isActive = client.User.IsActive
		log.Println("Toggle user active status:", isActive)
		return true
	}
	return false
}
