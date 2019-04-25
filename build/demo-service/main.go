package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	warppipe "github.com/perangel/warp-pipe"

	"gopkg.in/olahol/melody.v1"
)

var M = melody.New()

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

func WebSocketHandler(w http.ResponseWriter, r *http.Request) {
	M.HandleRequest(w, r)
}

func WebSocketMessageHandler(s *melody.Session, msg []byte) {
	M.Broadcast(msg)
}

func main() {
	http.HandleFunc("/", IndexHandler)
	http.HandleFunc("/ws", WebSocketHandler)
	M.HandleMessage(WebSocketMessageHandler)

	cfg, err := warppipe.NewConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	listener := warppipe.NewLogicalReplicationListener()
	wp := warppipe.NewWarpPipe(listener)
	err = wp.Open(&cfg.Database)
	if err != nil {
		log.Fatal(err)
	}

	changes, _ := wp.ListenForChanges(context.Background())
	go func() {
		for change := range changes {
			b, _ := json.Marshal(change)
			M.Broadcast(b)
		}
	}()

	log.Fatal(http.ListenAndServe(":8080", nil))
}
