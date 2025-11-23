package server

import (
	"log"
	"sync"
)

type Client struct {
}

type Hub struct {
	clients    map[string]*Client
	Broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mutex      sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[string]*Client),
		Broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) Run() {

	for {
		select {
		case client := <-h.register:
			h.mutex.Lock()
			log.Println("Register client:", client)
			h.clients["temp-id"] = client
			h.mutex.Unlock()
		case client := <-h.unregister:
			h.mutex.Lock()
			if _, ok := h.clients["temp-id"]; ok {
				delete(h.clients, "temp-id")
			}
			log.Println("Unregister client:", client)
			h.mutex.Unlock()
		case message := <-h.Broadcast:
			h.mutex.RLock()
			for id := range h.clients {
				log.Printf("Broadcast message: %s  id: %s\n", string(message), id)
			}
			h.mutex.RUnlock()
		}
	}
}
