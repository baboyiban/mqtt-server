package broker

import (
	"log"
	"net"

	"github.com/baboyiban/mqtt-server/internal/mqtt"
)

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
				if subClient, ok := c.broker.clients[subID]; ok && subID != clientID {
					_ = mqtt.WritePublishPacket(subClient.conn, pubPkt.Topic, pubPkt.Payload)
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
