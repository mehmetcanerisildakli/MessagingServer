package main

import (
	"MessagingServer/internal/server"
	"log"
	"net/http"

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

	defer func(connection *websocket.Conn) {
		err := connection.Close()
		if err != nil {

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

func main() {
	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("ws", wsHandler)

	port := ":8080"
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal("Connection Error: ", err)
	}
}
