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
	CheckOrigin: func(r *http.Request) bool {
		log.Printf("WebSocket Origin: %s", r.Header.Get("Origin"))
		return true
	},
	Subprotocols: []string{"mqtt", "mqttv3.1"},
}

func (b *Broker) ListenAndServeWS(addr string) error {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// CORS 헤더 추가
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// OPTIONS 요청 처리
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Log requested subprotocols
		if len(r.Header["Sec-Websocket-Protocol"]) > 0 {
			log.Printf("Client requested subprotocols: %v", r.Header["Sec-Websocket-Protocol"])
		}

		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WebSocket 업그레이드 실패:", err)
			return
		}
		log.Printf("WebSocket 연결 성공: %s", wsConn.RemoteAddr())
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
