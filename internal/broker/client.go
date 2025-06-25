package broker

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/baboyiban/mqtt-server/internal/mqtt"
	"github.com/gorilla/websocket"
)

// Clienter 인터페이스: TCP/WS 클라이언트 공통 동작
type Clienter interface {
	Handle()
	SendPublish(topic string, payload []byte) bool // 성공 여부 반환하도록 수정
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

// Client.SendPublish 메서드 변경
func (c *Client) SendPublish(topic string, payload []byte) bool {
	err := mqtt.WritePublishPacket(c.conn, topic, payload)
	if err != nil {
		log.Printf("[TCP] 메시지 전송 실패: id=%s, topic=%s, err=%v", formatClientID(c.id), topic, err)
		return false
	}
	return true
}

func (c *Client) Handle() {
	defer c.conn.Close()
	var clientID string

	for {
		header, err := mqtt.ReadPacketHeader(c.conn)
		if err != nil {
			id := formatClientID(c.id)
			log.Printf("[TCP] 연결 종료: id=%s, err=%v", id, err)
			break
		}

		switch header.Type {
		case mqtt.PacketTypeCONNECT:
			connectPkt, err := mqtt.ParseConnectPacket(c.conn, header.RemLen)
			if err != nil {
				log.Printf("[TCP] CONNECT 파싱 실패: err=%v", err)
				return
			}
			clientID = connectPkt.ClientID

			// 빈 ID 클라이언트에게 고유 ID 부여
			if clientID == "" {
				// 현재 시간 기반 해시 (6자리 16진수)
				clientID = fmt.Sprintf("auto-%x", time.Now().UnixNano()%0xFFFFFF)
				log.Printf("[TCP] 빈 ID 클라이언트에게 자동 ID 부여: id=%s", clientID)
			}

			c.id = clientID
			c.broker.mu.Lock()
			c.broker.clients[clientID] = c
			c.broker.mu.Unlock()
			log.Printf("[TCP] CONNECT: id=%s", formatClientID(clientID))
			_ = mqtt.WriteConnackPacket(c.conn)

		case mqtt.PacketTypeSUBSCRIBE:
			subPkt, err := mqtt.ParseSubscribePacket(c.conn, header.RemLen)
			if err != nil {
				log.Printf("[TCP] SUBSCRIBE 파싱 실패: id=%s, err=%v", formatClientID(c.id), err)
				return
			}
			c.broker.store.AddSubscriber(subPkt.Topic, clientID)
			c.subscribedTopics = append(c.subscribedTopics, subPkt.Topic)
			log.Printf("[TCP] SUBSCRIBE: id=%s, topic=%s", formatClientID(clientID), subPkt.Topic)
			_ = mqtt.WriteSubackPacket(c.conn, subPkt.PacketID)

		case mqtt.PacketTypePUBLISH:
			pubPkt, err := mqtt.ParsePublishPacket(c.conn, header.RemLen)
			if err != nil {
				log.Printf("[TCP] PUBLISH 파싱 실패: id=%s, err=%v", formatClientID(c.id), err)
				return
			}
			log.Printf("[TCP] PUBLISH: id=%s, topic=%s, payload=%s",
				formatClientID(clientID), pubPkt.Topic, string(pubPkt.Payload))

			// TCP 클라이언트 PUBLISH 핸들러 부분
			subs := c.broker.store.GetSubscribers(pubPkt.Topic)
			if len(subs) == 0 {
				log.Printf("[TCP] 구독자 없음: topic=%s", pubPkt.Topic)
			}

			// 메시지 전송 로직 개선
			for _, subID := range subs {
				c.broker.mu.RLock()
				subClient, ok := c.broker.clients[subID]
				c.broker.mu.RUnlock()

				if ok {
					log.Printf("[TCP] 메시지 전달: from=%s, to=%s, topic=%s",
						formatClientID(clientID), formatClientID(subID), pubPkt.Topic)

					// 메시지 전송 시도하고 결과 확인
					if !subClient.SendPublish(pubPkt.Topic, pubPkt.Payload) {
						// 전송 실패 - 해당 ID의 구독자만 제거
						log.Printf("[TCP] 구독자 연결 끊김, 제거: id=%s, topic=%s", formatClientID(subID), pubPkt.Topic)
						c.broker.mu.Lock()
						delete(c.broker.clients, subID)
						c.broker.mu.Unlock()
						c.broker.store.RemoveSubscriber(pubPkt.Topic, subID)
					}
				} else {
					// 클라이언트 객체가 없음 - 구독자 제거
					log.Printf("[TCP] 구독자 오프라인, 제거: id=%s, topic=%s", formatClientID(subID), pubPkt.Topic)
					c.broker.store.RemoveSubscriber(pubPkt.Topic, subID)
				}
			}

		case mqtt.PacketTypePINGREQ:
			log.Printf("[TCP] PING: id=%s", formatClientID(clientID))
			_ = mqtt.WritePingrespPacket(c.conn)

		case mqtt.PacketTypeDISCONNECT:
			log.Printf("[TCP] DISCONNECT: id=%s", formatClientID(clientID))
			break

		default:
			log.Printf("[TCP] 알 수 없는 패킷: id=%s, type=%d", formatClientID(c.id), header.Type)
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
		log.Printf("[TCP] 클라이언트 종료: id=%s", formatClientID(clientID))
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

func (c *WSClient) SendPublish(topic string, payload []byte) bool {
	var buf bytes.Buffer
	err := mqtt.WritePublishPacket(&buf, topic, payload)
	if err != nil {
		log.Printf("[WS] 패킷 생성 실패: id=%s, topic=%s, err=%v", formatClientID(c.id), topic, err)
		return false
	}

	err = c.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	if err != nil {
		log.Printf("[WS] 메시지 전송 실패: id=%s, topic=%s, err=%v", formatClientID(c.id), topic, err)
		return false
	}
	return true
}

func (c *WSClient) Handle() {
	defer c.conn.Close()
	var clientID string

	log.Printf("[WS] 연결 시작: addr=%s", c.conn.RemoteAddr())

	for {
		messageType, data, err := c.conn.ReadMessage()
		if err != nil {
			id := formatClientID(c.id)
			log.Printf("[WS] 연결 종료: id=%s, err=%v", id, err)
			break
		}

		// 메시지 타입/길이 검사
		if messageType != websocket.BinaryMessage {
			log.Printf("[WS] 비이진 메시지: id=%s, type=%d", formatClientID(c.id), messageType)
			continue
		}
		if len(data) == 0 {
			log.Printf("[WS] 빈 메시지: id=%s", formatClientID(c.id))
			continue
		}

		r := bytes.NewReader(data)
		header, err := mqtt.ReadPacketHeader(r)
		if err != nil {
			log.Printf("[WS] 헤더 파싱 실패: id=%s, err=%v", formatClientID(c.id), err)
			break
		}

		switch header.Type {
		case mqtt.PacketTypeCONNECT:
			connectPkt, err := mqtt.ParseConnectPacket(r, header.RemLen)
			if err != nil {
				log.Printf("[WS] CONNECT 파싱 실패: err=%v", err)
				return
			}
			clientID = connectPkt.ClientID

			// 빈 ID 클라이언트에게 고유 ID 부여
			if clientID == "" {
				// 현재 시간 기반 해시 (6자리 16진수)
				clientID = fmt.Sprintf("auto-%x", time.Now().UnixNano()%0xFFFFFF)
				log.Printf("[TCP] 빈 ID 클라이언트에게 자동 ID 부여: id=%s", clientID)
			}

			c.id = clientID
			c.broker.mu.Lock()
			c.broker.clients[clientID] = c
			c.broker.mu.Unlock()
			log.Printf("[WS] CONNECT: id=%s", formatClientID(clientID))

			var buf bytes.Buffer
			_ = mqtt.WriteConnackPacket(&buf)
			_ = c.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())

		case mqtt.PacketTypeSUBSCRIBE:
			subPkt, err := mqtt.ParseSubscribePacket(r, header.RemLen)
			if err != nil {
				log.Printf("[WS] SUBSCRIBE 파싱 실패: id=%s, err=%v", formatClientID(c.id), err)
				return
			}
			c.broker.store.AddSubscriber(subPkt.Topic, clientID)
			c.subscribedTopics = append(c.subscribedTopics, subPkt.Topic)
			log.Printf("[WS] SUBSCRIBE: id=%s, topic=%s", formatClientID(clientID), subPkt.Topic)

			var buf bytes.Buffer
			_ = mqtt.WriteSubackPacket(&buf, subPkt.PacketID)
			_ = c.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())

		case mqtt.PacketTypePUBLISH:
			pubPkt, err := mqtt.ParsePublishPacket(r, header.RemLen)
			if err != nil {
				log.Printf("[WS] PUBLISH 파싱 실패: id=%s, err=%v", formatClientID(c.id), err)
				return
			}
			log.Printf("[WS] PUBLISH: id=%s, topic=%s, payload=%s",
				formatClientID(clientID), pubPkt.Topic, string(pubPkt.Payload))

			// TCP 클라이언트 PUBLISH 핸들러 부분
			subs := c.broker.store.GetSubscribers(pubPkt.Topic)
			if len(subs) == 0 {
				log.Printf("[TCP] 구독자 없음: topic=%s", pubPkt.Topic)
			}

			// 메시지 전송 로직 개선
			for _, subID := range subs {
				c.broker.mu.RLock()
				subClient, ok := c.broker.clients[subID]
				c.broker.mu.RUnlock()

				if ok {
					log.Printf("[TCP] 메시지 전달: from=%s, to=%s, topic=%s",
						formatClientID(clientID), formatClientID(subID), pubPkt.Topic)

					// 메시지 전송 시도하고 결과 확인
					if !subClient.SendPublish(pubPkt.Topic, pubPkt.Payload) {
						// 전송 실패 - 해당 ID의 구독자만 제거
						log.Printf("[TCP] 구독자 연결 끊김, 제거: id=%s, topic=%s", formatClientID(subID), pubPkt.Topic)
						c.broker.mu.Lock()
						delete(c.broker.clients, subID)
						c.broker.mu.Unlock()
						c.broker.store.RemoveSubscriber(pubPkt.Topic, subID)
					}
				} else {
					// 클라이언트 객체가 없음 - 구독자 제거
					log.Printf("[TCP] 구독자 오프라인, 제거: id=%s, topic=%s", formatClientID(subID), pubPkt.Topic)
					c.broker.store.RemoveSubscriber(pubPkt.Topic, subID)
				}
			}

		case mqtt.PacketTypePINGREQ:
			log.Printf("[WS] PING: id=%s", formatClientID(clientID))
			var buf bytes.Buffer
			_ = mqtt.WritePingrespPacket(&buf)
			_ = c.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())

		case mqtt.PacketTypeDISCONNECT:
			log.Printf("[WS] DISCONNECT: id=%s", formatClientID(clientID))
			break

		default:
			log.Printf("[WS] 알 수 없는 패킷: id=%s, type=%d", formatClientID(c.id), header.Type)
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
		log.Printf("[WS] 클라이언트 종료: id=%s", formatClientID(clientID))
	}
}

// formatClientID는 빈 클라이언트 ID를 "(없음)"으로 표시합니다
func formatClientID(id string) string {
	if id == "" {
		return "(없음)"
	}
	return id
}
