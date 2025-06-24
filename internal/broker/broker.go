package broker

import (
	"log"
	"net"
	"sync"

	"github.com/baboyiban/mqtt-server/internal/storage"
)

type Broker struct {
	store   storage.Store
	clients map[string]*Client
	mu      sync.RWMutex
}

func NewBroker(store storage.Store) *Broker {
	return &Broker{
		store:   store,
		clients: make(map[string]*Client),
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

func (b *Broker) handleClient(conn net.Conn) {
	client := NewClient(conn, b)
	client.Handle()
}
