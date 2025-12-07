package main

import (
	"MessagingServer/internal/models"
	"MessagingServer/internal/server"
	"encoding/json"
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
	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "Missing username", http.StatusBadRequest)
		return
	}
	connection, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket Loading err: ", err)
		return
	}

	tempUser := &models.User{
		ID:       strconv.FormatInt(time.Now().UnixNano(), 10),
		Username: username,
		IsActive: true,
		Role:     models.RoleUser,
	}
	client := server.NewClient(tempUser, globalHub, connection)

	globalHub.Register <- client
	go client.WritePump()
	go client.ReadPump()

	time.Sleep(10 * time.Millisecond)

	welcomeMsg := models.ClientMessage{
		Type:       models.MsgTypeStatus,
		Content:    fmt.Sprintf("Welcome %s!", username),
		Sender:     tempUser.ID,
		SenderName: username,
		Timestamp:  time.Now(),
	}
	welcomeBytes, _ := json.Marshal(welcomeMsg)
	client.Send <- welcomeBytes
	sendUserList(client)
	notifyUserJoined(tempUser)
}

func sendUserList(client *server.Client) {
	users := globalHub.GetAllUsers()
	userListMsg := models.ClientMessage{
		Type:      models.MsgTypeControl,
		Content:   "user_list",
		Timestamp: time.Now(),
	}
	userJson, _ := json.Marshal(users)
	userListMsg.Content = string(userJson)
	userListBytes, _ := json.Marshal(userListMsg)
	client.Send <- userListBytes
}

func notifyUserJoined(user *models.User) {
	msg := models.ClientMessage{
		Type:       models.MsgTypeStatus,
		Content:    fmt.Sprintf("User Joined %s", user.Username),
		Sender:     user.ID,
		SenderName: user.Username,
		Timestamp:  time.Now(),
	}
	msgBytes, _ := json.Marshal(msg)
	globalHub.Broadcast <- msgBytes
}

func adminStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	targetUserId := r.URL.Query().Get("id")
	if targetUserId == "" {
		http.Error(w, "Target user id is empty", http.StatusBadRequest)
		return
	}

	activeStatus := r.URL.Query().Get("active")
	if activeStatus == "" {
		http.Error(w, "Active status is empty", http.StatusBadRequest)
		return
	}

	isActive := activeStatus == "true"
	if globalHub.ToggleUserActiveStatus(targetUserId, isActive) {
		w.WriteHeader(http.StatusOK)
		_, err := io.WriteString(w, fmt.Sprintf(" User: %s is Active: %t", targetUserId, isActive))
		if err != nil {
			return
		}
	} else {
		http.Error(w, "User is not active", http.StatusBadRequest)
	}
}

func getUsersHandler(w http.ResponseWriter, r *http.Request) {
	users := globalHub.GetAllUsers()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(users)
}

func getUserHandler(w http.ResponseWriter, r *http.Request) {
	userId := r.URL.Query().Get("id")
	if userId == "" {
		http.Error(w, "User id is empty", http.StatusBadRequest)
		return
	}
	user := globalHub.GetUser(userId)
	if user == nil {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(user)
}

func main() {
	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/api/users", getUsersHandler)
	http.HandleFunc("/api/user", getUserHandler)
	http.HandleFunc("/admin/user-status", adminStatusHandler)

	port := ":8080"
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal("Connection Error: ", err)
	}
}
