package broker

import (
	"bytes"
	"log"
	"net"

	"github.com/baboyiban/mqtt-server/internal/mqtt"
	"github.com/gorilla/websocket"
)

// Clienter 인터페이스: TCP/WS 클라이언트 공통 동작
type Clienter interface {
	Handle()
	SendPublish(topic string, payload []byte)
	GetID() string
}

// --- TCP 클라이언트 ---

type Client struct {
	conn   net.Conn
	broker *Broker
	id     string
}

func NewClient(conn net.Conn, broker *Broker) *Client {
	return &Client{
		conn:   conn,
		broker: broker,
	}
}

func (c *Client) GetID() string {
	return c.id
}

func (c *Client) SendPublish(topic string, payload []byte) {
	_ = mqtt.WritePublishPacket(c.conn, topic, payload)
}

func (c *Client) Handle() {
	defer c.conn.Close()
	var clientID string

	for {
		header, err := mqtt.ReadPacketHeader(c.conn)
		if err != nil {
			log.Printf("패킷 헤더 읽기 실패: %v", err)
			break
		}

		switch header.Type {
		case mqtt.PacketTypeCONNECT:
			connectPkt, err := mqtt.ParseConnectPacket(c.conn, header.RemLen)
			if err != nil {
				log.Printf("CONNECT 패킷 파싱 실패: %v", err)
				return
			}
			clientID = connectPkt.ClientID
			c.id = clientID
			c.broker.mu.Lock()
			c.broker.clients[clientID] = c
			c.broker.mu.Unlock()
			log.Printf("클라이언트 ID: %s", clientID)
			_ = mqtt.WriteConnackPacket(c.conn)

		case mqtt.PacketTypeSUBSCRIBE:
			subPkt, err := mqtt.ParseSubscribePacket(c.conn, header.RemLen)
			if err != nil {
				log.Printf("SUBSCRIBE 패킷 파싱 실패: %v", err)
				return
			}
			c.broker.store.AddSubscriber(subPkt.Topic, clientID)
			log.Printf("구독: %s → %s", clientID, subPkt.Topic)
			_ = mqtt.WriteSubackPacket(c.conn, subPkt.PacketID)

		case mqtt.PacketTypePUBLISH:
			pubPkt, err := mqtt.ParsePublishPacket(c.conn, header.RemLen)
			if err != nil {
				log.Printf("PUBLISH 패킷 파싱 실패: %v", err)
				return
			}
			log.Printf("메시지 발행: [%s] %s", pubPkt.Topic, string(pubPkt.Payload))
			subs := c.broker.store.GetSubscribers(pubPkt.Topic)
			for _, subID := range subs {
				if subID == clientID {
					continue // 자기 자신에게는 보내지 않음
				}
				c.broker.mu.RLock()
				subClient, ok := c.broker.clients[subID]
				c.broker.mu.RUnlock()
				if ok {
					subClient.SendPublish(pubPkt.Topic, pubPkt.Payload)
				}
			}

		case mqtt.PacketTypeDISCONNECT:
			log.Printf("클라이언트 %s DISCONNECT", clientID)
			break

		default:
			log.Printf("알 수 없는 패킷 타입: %d", header.Type)
			break
		}
	}

	if clientID != "" {
		c.broker.mu.Lock()
		delete(c.broker.clients, clientID)
		c.broker.mu.Unlock()
	}
}

// --- WebSocket 클라이언트 ---

type WSClient struct {
	conn   *websocket.Conn
	broker *Broker
	id     string
}

func NewWSClient(conn *websocket.Conn, broker *Broker) *WSClient {
	return &WSClient{
		conn:   conn,
		broker: broker,
	}
}

func (c *WSClient) GetID() string {
	return c.id
}

func (c *WSClient) SendPublish(topic string, payload []byte) {
	var buf bytes.Buffer
	_ = mqtt.WritePublishPacket(&buf, topic, payload)
	_ = c.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
}

func (c *WSClient) Handle() {
	defer c.conn.Close()
	var clientID string

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("WebSocket 읽기 실패: %v", err)
			break
		}
		r := bytes.NewReader(data)
		header, err := mqtt.ReadPacketHeader(r)
		if err != nil {
			log.Printf("패킷 헤더 읽기 실패: %v", err)
			break
		}

		switch header.Type {
		case mqtt.PacketTypeCONNECT:
			connectPkt, err := mqtt.ParseConnectPacket(r, header.RemLen)
			if err != nil {
				log.Printf("CONNECT 패킷 파싱 실패: %v", err)
				return
			}
			clientID = connectPkt.ClientID
			c.id = clientID
			c.broker.mu.Lock()
			c.broker.clients[clientID] = c
			c.broker.mu.Unlock()
			log.Printf("WS 클라이언트 ID: %s", clientID)
			var buf bytes.Buffer
			_ = mqtt.WriteConnackPacket(&buf)
			_ = c.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())

		case mqtt.PacketTypeSUBSCRIBE:
			subPkt, err := mqtt.ParseSubscribePacket(r, header.RemLen)
			if err != nil {
				log.Printf("SUBSCRIBE 패킷 파싱 실패: %v", err)
				return
			}
			c.broker.store.AddSubscriber(subPkt.Topic, clientID)
			log.Printf("WS 구독: %s → %s", clientID, subPkt.Topic)
			var buf bytes.Buffer
			_ = mqtt.WriteSubackPacket(&buf, subPkt.PacketID)
			_ = c.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())

		case mqtt.PacketTypePUBLISH:
			pubPkt, err := mqtt.ParsePublishPacket(r, header.RemLen)
			if err != nil {
				log.Printf("PUBLISH 패킷 파싱 실패: %v", err)
				return
			}
			log.Printf("WS 메시지 발행: [%s] %s", pubPkt.Topic, string(pubPkt.Payload))
			subs := c.broker.store.GetSubscribers(pubPkt.Topic)
			for _, subID := range subs {
				if subID == clientID {
					continue // 자기 자신에게는 보내지 않음
				}
				c.broker.mu.RLock()
				subClient, ok := c.broker.clients[subID]
				c.broker.mu.RUnlock()
				if ok {
					subClient.SendPublish(pubPkt.Topic, pubPkt.Payload)
				}
			}

		case mqtt.PacketTypeDISCONNECT:
			log.Printf("WS 클라이언트 %s DISCONNECT", clientID)
			break

		default:
			log.Printf("알 수 없는 패킷 타입: %d", header.Type)
			break
		}
	}

	if clientID != "" {
		c.broker.mu.Lock()
		delete(c.broker.clients, clientID)
		c.broker.mu.Unlock()
	}
}
