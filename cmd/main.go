package main

import (
	"MessagingServer/internal/models"
	"MessagingServer/internal/server"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

var globalHub = server.NewHub()

func init() {
	go globalHub.Run()
	log.Println("Hub goroutine is started.")
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	connection, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket Loading err: ", err)
		return
	}

	tempUser := &models.User{
		ID:       strconv.FormatInt(time.Now().UnixNano(), 10),
		Username: fmt.Sprintf("User:%d", time.Now().UnixNano()),
		IsActive: true,
		Role:     models.RoleUser,
	}
	client := server.NewClient(tempUser, globalHub, connection)

	globalHub.Register <- client
	go client.WritePump()
	go client.ReadPump()

	defer func(connection *websocket.Conn) {
		err := connection.Close()
		if err != nil {
			log.Println(err)
		}
	}(connection)

	err = connection.WriteMessage(websocket.TextMessage, []byte("ok :) Welcome to server"))
	if err != nil {
		return
	}
	for {
		messageType, message, err := connection.ReadMessage()
		if err != nil {
			log.Println("WebSocket Read err: ", err)
			break
		}

		log.Printf("The Message: %s  type: %s", message, messageType)
		globalHub.Broadcast <- message
	}
}

func adminStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	targetUserId := r.URL.Query().Get("id")
	if targetUserId == "" {
		http.Error(w, "Target user id is empty", http.StatusBadRequest)
	}

	activeStatus := r.URL.Query().Get("active")
	if activeStatus == "" {
		http.Error(w, "Active status is empty", http.StatusBadRequest)
	}

	isActive := (activeStatus == "true")
	if globalHub.ToggleUserActiveStatus(targetUserId, isActive) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, fmt.Sprintf(" User: %s is Active: %t", targetUserId, isActive))
	} else {
		http.Error(w, "User is not active", http.StatusBadRequest)
	}
}

func main() {
	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/admin/user-status", adminStatusHandler)

	port := ":8080"
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal("Connection Error: ", err)
	}
}
