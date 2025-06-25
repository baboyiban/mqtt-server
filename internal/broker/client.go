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
	conn             net.Conn
	broker           *Broker
	id               string
	subscribedTopics []string
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

// ...생략...
func (c *Client) Handle() {
	defer c.conn.Close()
	var clientID string

	for {
		header, err := mqtt.ReadPacketHeader(c.conn)
		if err != nil {
			log.Printf("[TCP] %s: 패킷 헤더 읽기 실패: %v", c.id, err)
			break
		}

		switch header.Type {
		case mqtt.PacketTypeCONNECT:
			connectPkt, err := mqtt.ParseConnectPacket(c.conn, header.RemLen)
			if err != nil {
				log.Printf("[TCP] CONNECT 패킷 파싱 실패: %v", err)
				return
			}
			clientID = connectPkt.ClientID
			c.id = clientID
			c.broker.mu.Lock()
			c.broker.clients[clientID] = c
			c.broker.mu.Unlock()
			log.Printf("[TCP] CONNECT: id=%s", clientID)
			_ = mqtt.WriteConnackPacket(c.conn)

		case mqtt.PacketTypeSUBSCRIBE:
			subPkt, err := mqtt.ParseSubscribePacket(c.conn, header.RemLen)
			if err != nil {
				log.Printf("[TCP] %s: SUBSCRIBE 패킷 파싱 실패: %v", c.id, err)
				return
			}
			c.broker.store.AddSubscriber(subPkt.Topic, clientID)
			c.subscribedTopics = append(c.subscribedTopics, subPkt.Topic)
			log.Printf("[TCP] SUBSCRIBE: id=%s topic=%s", clientID, subPkt.Topic)
			_ = mqtt.WriteSubackPacket(c.conn, subPkt.PacketID)

		case mqtt.PacketTypePUBLISH:
			pubPkt, err := mqtt.ParsePublishPacket(c.conn, header.RemLen)
			if err != nil {
				log.Printf("[TCP] %s: PUBLISH 패킷 파싱 실패: %v", c.id, err)
				return
			}
			log.Printf("[TCP] PUBLISH: id=%s topic=%s payload=%s", clientID, pubPkt.Topic, string(pubPkt.Payload))
			subs := c.broker.store.GetSubscribers(pubPkt.Topic)
			for _, subID := range subs {
				c.broker.mu.RLock()
				subClient, ok := c.broker.clients[subID]
				c.broker.mu.RUnlock()
				if ok {
					log.Printf("[TCP] DELIVER: %s → %s topic=%s", clientID, subID, pubPkt.Topic)
					subClient.SendPublish(pubPkt.Topic, pubPkt.Payload)
				} else {
					log.Printf("[TCP] %s: 구독자 %s 없음", clientID, subID)
				}
			}

		case mqtt.PacketTypePINGREQ:
			log.Printf("[TCP] PINGREQ: id=%s", clientID)
			_ = mqtt.WritePingrespPacket(c.conn)
			log.Printf("[TCP] PINGRESP: id=%s", clientID)

		case mqtt.PacketTypeDISCONNECT:
			log.Printf("[TCP] DISCONNECT: id=%s", clientID)
			break

		default:
			log.Printf("[TCP] %s: 알 수 없는 패킷 타입: %d", c.id, header.Type)
			break
		}
	}

	if clientID != "" {
		c.broker.mu.Lock()
		delete(c.broker.clients, clientID)
		c.broker.mu.Unlock()
		for _, topic := range c.subscribedTopics {
			c.broker.store.RemoveSubscriber(topic, clientID)
		}
		log.Printf("[TCP] CLOSE: id=%s", clientID)
	}
}

// --- WebSocket 클라이언트 ---

type WSClient struct {
	conn             *websocket.Conn
	broker           *Broker
	id               string
	subscribedTopics []string
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

// ...생략...
func (c *WSClient) Handle() {
	defer c.conn.Close()
	var clientID string

	log.Printf("[WS] 연결: %s", c.conn.RemoteAddr())

	for {
		messageType, data, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("[WS] %s: 읽기 실패: %v", c.id, err)
			break
		}
		if messageType != websocket.BinaryMessage {
			log.Printf("[WS] %s: 바이너리 아님(type=%d)", c.id, messageType)
			continue
		}
		if len(data) == 0 {
			log.Printf("[WS] %s: 빈 메시지", c.id)
			continue
		}

		r := bytes.NewReader(data)
		header, err := mqtt.ReadPacketHeader(r)
		if err != nil {
			log.Printf("[WS] %s: 패킷 헤더 읽기 실패: %v", c.id, err)
			break
		}

		switch header.Type {
		case mqtt.PacketTypeCONNECT:
			connectPkt, err := mqtt.ParseConnectPacket(r, header.RemLen)
			if err != nil {
				log.Printf("[WS] CONNECT 패킷 파싱 실패: %v", err)
				return
			}
			clientID = connectPkt.ClientID
			c.id = clientID
			c.broker.mu.Lock()
			c.broker.clients[clientID] = c
			c.broker.mu.Unlock()
			log.Printf("[WS] CONNECT: id=%s", clientID)
			var buf bytes.Buffer
			_ = mqtt.WriteConnackPacket(&buf)
			_ = c.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())

		case mqtt.PacketTypeSUBSCRIBE:
			subPkt, err := mqtt.ParseSubscribePacket(r, header.RemLen)
			if err != nil {
				log.Printf("[WS] %s: SUBSCRIBE 패킷 파싱 실패: %v", c.id, err)
				return
			}
			c.broker.store.AddSubscriber(subPkt.Topic, clientID)
			c.subscribedTopics = append(c.subscribedTopics, subPkt.Topic)
			log.Printf("[WS] SUBSCRIBE: id=%s topic=%s", clientID, subPkt.Topic)
			var buf bytes.Buffer
			_ = mqtt.WriteSubackPacket(&buf, subPkt.PacketID)
			_ = c.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())

		case mqtt.PacketTypePUBLISH:
			pubPkt, err := mqtt.ParsePublishPacket(r, header.RemLen)
			if err != nil {
				log.Printf("[WS] %s: PUBLISH 패킷 파싱 실패: %v", c.id, err)
				return
			}
			log.Printf("[WS] PUBLISH: id=%s topic=%s payload=%s", clientID, pubPkt.Topic, string(pubPkt.Payload))
			subs := c.broker.store.GetSubscribers(pubPkt.Topic)
			for _, subID := range subs {
				c.broker.mu.RLock()
				subClient, ok := c.broker.clients[subID]
				c.broker.mu.RUnlock()
				if ok {
					log.Printf("[WS] DELIVER: %s → %s topic=%s", clientID, subID, pubPkt.Topic)
					subClient.SendPublish(pubPkt.Topic, pubPkt.Payload)
				}
			}

		case mqtt.PacketTypePINGREQ:
			log.Printf("[WS] PINGREQ: id=%s", clientID)
			var buf bytes.Buffer
			_ = mqtt.WritePingrespPacket(&buf)
			_ = c.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
			log.Printf("[WS] PINGRESP: id=%s", clientID)

		case mqtt.PacketTypeDISCONNECT:
			log.Printf("[WS] DISCONNECT: id=%s", clientID)
			break

		default:
			log.Printf("[WS] %s: 알 수 없는 패킷 타입: %d", c.id, header.Type)
			break
		}
	}

	if clientID != "" {
		c.broker.mu.Lock()
		delete(c.broker.clients, clientID)
		c.broker.mu.Unlock()
		for _, topic := range c.subscribedTopics {
			c.broker.store.RemoveSubscriber(topic, clientID)
		}
		log.Printf("[WS] CLOSE: id=%s", clientID)
	}
}
