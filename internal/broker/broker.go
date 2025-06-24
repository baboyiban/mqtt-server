package broker

import (
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/baboyiban/mqtt-server/internal/storage"
	"github.com/gorilla/websocket"
)

type Broker struct {
	store   storage.Store
	clients map[string]Clienter
	mu      sync.RWMutex
}

func NewBroker(store storage.Store) *Broker {
	return &Broker{
		store:   store,
		clients: make(map[string]Clienter),
	}
}

func (b *Broker) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Printf("MQTT 서버 시작: %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("클라이언트 연결 실패: %v", err)
			continue
		}
		go b.handleClient(conn)
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (b *Broker) ListenAndServeWS(addr string) error {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WebSocket 업그레이드 실패:", err)
			return
		}
		go b.handleWSClient(wsConn)
	})
	log.Printf("MQTT WebSocket 서버 시작: %s", addr)
	return http.ListenAndServe(addr, nil)
}

func (b *Broker) handleClient(conn net.Conn) {
	client := NewClient(conn, b)
	client.Handle()
}

func (b *Broker) handleWSClient(wsConn *websocket.Conn) {
	client := NewWSClient(wsConn, b)
	client.Handle()
}
