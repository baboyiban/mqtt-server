package broker

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/baboyiban/mqtt-server/internal/mqtt"
	"github.com/gorilla/websocket"
)

// ClientTransport 인터페이스는 클라이언트 통신 레이어를 추상화합니다.
type ClientTransport interface {
	io.ReadWriter
	Close() error
	RemoteAddr() net.Addr
	Type() string // "TCP" 또는 "WS"를 반환
}

// Clienter 인터페이스: 모든 클라이언트 공통 동작
type Clienter interface {
	Handle()
	SendPublish(topic string, payload []byte) bool
	GetID() string
	GetTransport() ClientTransport
}

// TCPTransport는 TCP 연결에 대한 ClientTransport 구현체
type TCPTransport struct {
	conn net.Conn
}

func NewTCPTransport(conn net.Conn) *TCPTransport {
	return &TCPTransport{conn: conn}
}

func (t *TCPTransport) Read(p []byte) (n int, err error)  { return t.conn.Read(p) }
func (t *TCPTransport) Write(p []byte) (n int, err error) { return t.conn.Write(p) }
func (t *TCPTransport) Close() error                      { return t.conn.Close() }
func (t *TCPTransport) RemoteAddr() net.Addr              { return t.conn.RemoteAddr() }
func (t *TCPTransport) Type() string                      { return "TCP" }

// WSTransport는 WebSocket 연결에 대한 ClientTransport 구현체
type WSTransport struct {
	conn     *websocket.Conn
	reader   io.Reader
	readBuf  []byte
	writeBuf bytes.Buffer
}

func NewWSTransport(conn *websocket.Conn) *WSTransport {
	return &WSTransport{conn: conn}
}

func (t *WSTransport) Read(p []byte) (n int, err error) {
	if t.reader == nil {
		messageType, r, err := t.conn.NextReader()
		if err != nil {
			return 0, err
		}
		if messageType != websocket.BinaryMessage {
			return 0, fmt.Errorf("expected binary message, got %d", messageType)
		}
		t.reader = r
	}
	n, err = t.reader.Read(p)
	if err == io.EOF {
		t.reader = nil
		err = nil
	}
	return n, err
}

func (t *WSTransport) Write(p []byte) (n int, err error) {
	t.writeBuf.Reset()
	n, err = t.writeBuf.Write(p)
	if err != nil {
		return n, err
	}
	err = t.conn.WriteMessage(websocket.BinaryMessage, t.writeBuf.Bytes())
	return n, err
}

func (t *WSTransport) Close() error         { return t.conn.Close() }
func (t *WSTransport) RemoteAddr() net.Addr { return t.conn.RemoteAddr() }
func (t *WSTransport) Type() string         { return "WS" }

// BaseClient는 TCP 및 WS 클라이언트의 공통 필드와 기능을 담당합니다
type BaseClient struct {
	transport        ClientTransport
	broker           *Broker
	id               string
	subscribedTopics []string
}

// NewClient는 TCP 클라이언트를 생성합니다
func NewClient(conn net.Conn, broker *Broker) Clienter {
	return &BaseClient{
		transport: NewTCPTransport(conn),
		broker:    broker,
	}
}

// NewWSClient는 WebSocket 클라이언트를 생성합니다
func NewWSClient(conn *websocket.Conn, broker *Broker) Clienter {
	return &BaseClient{
		transport: NewWSTransport(conn),
		broker:    broker,
	}
}

func (c *BaseClient) GetID() string {
	return c.id
}

func (c *BaseClient) GetTransport() ClientTransport {
	return c.transport
}

// 공통 publish 기능
func (c *BaseClient) SendPublish(topic string, payload []byte) bool {
	err := mqtt.WritePublishPacket(c.transport, topic, payload)
	if err != nil {
		log.Printf("[%s] 메시지 전송 실패: id=%s, topic=%s, err=%v",
			c.transport.Type(), formatClientID(c.id), topic, err)
		return false
	}
	return true
}

// --- 리팩토링된 Handle 및 패킷 분기 ---

func (c *BaseClient) Handle() {
	defer c.transport.Close()
	var clientID string

	transportType := c.transport.Type()
	log.Printf("[%s] 연결 시작: addr=%s", transportType, c.transport.RemoteAddr())

	for {
		header, err := mqtt.ReadPacketHeader(c.transport)
		if err != nil {
			c.logDisconnect(err)
			break
		}
		if c.handlePacket(header, &clientID) {
			break
		}
	}

	c.cleanupClient(clientID)
}

func (c *BaseClient) handlePacket(header *mqtt.PacketHeader, clientID *string) (shouldBreak bool) {
	switch header.Type {
	case mqtt.PacketTypeCONNECT:
		*clientID = c.handleConnect(header)
		if *clientID == "" {
			return true
		}
	case mqtt.PacketTypeSUBSCRIBE:
		if !c.handleSubscribe(header, *clientID) {
			return true
		}
	case mqtt.PacketTypePUBLISH:
		if !c.handlePublish(header, *clientID) {
			return true
		}
	case mqtt.PacketTypePINGREQ:
		if !c.handlePingReq(*clientID) {
			return true
		}
	case mqtt.PacketTypeDISCONNECT:
		log.Printf("[%s] DISCONNECT: id=%s", c.transport.Type(), formatClientID(*clientID))
		return true
	default:
		log.Printf("[%s] 알 수 없는 패킷: id=%s, type=%d",
			c.transport.Type(), formatClientID(c.id), header.Type)
	}
	return false
}

func (c *BaseClient) logDisconnect(err error) {
	id := formatClientID(c.id)
	log.Printf("[%s] 연결 종료: id=%s, err=%v", c.transport.Type(), id, err)
}

// --- 리팩토링된 handleConnect ---

func (c *BaseClient) handleConnect(header *mqtt.PacketHeader) string {
	connectPkt, err := c.parseConnectPacket(header)
	if err != nil {
		return ""
	}
	clientID := c.assignClientID(connectPkt.ClientID)
	c.registerClient(clientID)
	if !c.sendConnack(clientID) {
		return ""
	}
	return clientID
}

func (c *BaseClient) parseConnectPacket(header *mqtt.PacketHeader) (*mqtt.ConnectPacket, error) {
	connectPkt, err := mqtt.ParseConnectPacket(c.transport, header.RemLen)
	if err != nil {
		log.Printf("[%s] CONNECT 파싱 실패: err=%v", c.transport.Type(), err)
		return nil, err
	}
	return connectPkt, nil
}

func (c *BaseClient) assignClientID(clientID string) string {
	if clientID == "" {
		clientID = fmt.Sprintf("auto-%x", time.Now().UnixNano()%0xFFFFFF)
		log.Printf("[%s] 빈 ID 클라이언트에게 자동 ID 부여: id=%s", c.transport.Type(), clientID)
	}
	c.id = clientID
	return clientID
}

func (c *BaseClient) registerClient(clientID string) {
	c.broker.mu.Lock()
	c.broker.clients[clientID] = c
	c.broker.mu.Unlock()
	log.Printf("[%s] CONNECT: id=%s", c.transport.Type(), formatClientID(clientID))
}

func (c *BaseClient) sendConnack(clientID string) bool {
	err := mqtt.WriteConnackPacket(c.transport)
	if err != nil {
		log.Printf("[%s] CONNACK 전송 실패: id=%s, err=%v", c.transport.Type(), formatClientID(clientID), err)
		return false
	}
	return true
}

// --- 리팩토링된 handleSubscribe ---

func (c *BaseClient) handleSubscribe(header *mqtt.PacketHeader, clientID string) bool {
	subPkt, err := c.parseSubscribePacket(header)
	if err != nil {
		return false
	}
	c.saveSubscription(subPkt.Topic, clientID)
	c.subscribedTopics = append(c.subscribedTopics, subPkt.Topic)
	log.Printf("[%s] SUBSCRIBE: id=%s, topic=%s", c.transport.Type(), formatClientID(clientID), subPkt.Topic)
	return c.sendSuback(subPkt.PacketID, clientID, subPkt.Topic)
}

func (c *BaseClient) parseSubscribePacket(header *mqtt.PacketHeader) (*mqtt.SubscribePacket, error) {
	subPkt, err := mqtt.ParseSubscribePacket(c.transport, header.RemLen)
	if err != nil {
		log.Printf("[%s] SUBSCRIBE 파싱 실패: id=%s, err=%v",
			c.transport.Type(), formatClientID(c.id), err)
		return nil, err
	}
	return subPkt, nil
}

func (c *BaseClient) saveSubscription(topic, clientID string) {
	err := c.broker.store.AddSubscriber(topic, clientID)
	if err != nil {
		log.Printf("[%s] 구독 저장 실패: id=%s, topic=%s, err=%v",
			c.transport.Type(), formatClientID(clientID), topic, err)
	}
}

func (c *BaseClient) sendSuback(packetID uint16, clientID, topic string) bool {
	err := mqtt.WriteSubackPacket(c.transport, packetID)
	if err != nil {
		log.Printf("[%s] SUBACK 전송 실패: id=%s, topic=%s, err=%v",
			c.transport.Type(), formatClientID(clientID), topic, err)
		return false
	}
	return true
}

// --- 리팩토링된 handlePublish ---

func (c *BaseClient) handlePublish(header *mqtt.PacketHeader, clientID string) bool {
	pubPkt, err := c.parsePublishPacket(header)
	if err != nil {
		return false
	}
	c.logPublish(clientID, pubPkt.Topic, pubPkt.Payload)
	c.deliverMessageToSubscribers(clientID, pubPkt.Topic, pubPkt.Payload)
	return true
}

func (c *BaseClient) parsePublishPacket(header *mqtt.PacketHeader) (*mqtt.PublishPacket, error) {
	pubPkt, err := mqtt.ParsePublishPacket(c.transport, header.RemLen)
	if err != nil {
		log.Printf("[%s] PUBLISH 파싱 실패: id=%s, err=%v",
			c.transport.Type(), formatClientID(c.id), err)
		return nil, err
	}
	return pubPkt, nil
}

func (c *BaseClient) logPublish(clientID, topic string, payload []byte) {
	log.Printf("[%s] PUBLISH: id=%s, topic=%s, payload=%s",
		c.transport.Type(), formatClientID(clientID), topic, string(payload))
}

// --- 이하 기존 함수 동일 ---

func (c *BaseClient) handlePingReq(clientID string) bool {
	transportType := c.transport.Type()

	log.Printf("[%s] PING: id=%s", transportType, formatClientID(clientID))
	err := mqtt.WritePingrespPacket(c.transport)
	if err != nil {
		log.Printf("[%s] PINGRESP 전송 실패: id=%s, err=%v",
			transportType, formatClientID(clientID), err)
		return false
	}

	return true
}

// cleanupClient는 클라이언트 연결 종료 시 정리 작업을 수행합니다
func (c *BaseClient) cleanupClient(clientID string) {
	if clientID == "" {
		return
	}

	c.broker.mu.Lock()
	delete(c.broker.clients, clientID)
	c.broker.mu.Unlock()

	for _, topic := range c.subscribedTopics {
		err := c.broker.store.RemoveSubscriber(topic, clientID)
		if err != nil {
			log.Printf("[%s] 구독 삭제 실패: id=%s, topic=%s, err=%v",
				c.transport.Type(), formatClientID(clientID), topic, err)
		}
	}

	log.Printf("[%s] 클라이언트 종료: id=%s", c.transport.Type(), formatClientID(clientID))
}

// deliverMessageToSubscribers는 발행된 메시지를 모든 구독자에게 전달합니다
func (c *BaseClient) deliverMessageToSubscribers(publisherID, topic string, payload []byte) {
	subs := c.broker.store.GetSubscribers(topic)
	if len(subs) == 0 {
		log.Printf("[%s] 구독자 없음: topic=%s", c.transport.Type(), topic)
		return
	}
	for _, subID := range subs {
		c.deliverMessageToSubscriber(publisherID, topic, payload, subID)
	}
}

func (c *BaseClient) deliverMessageToSubscriber(publisherID, topic string, payload []byte, subID string) {
	transportType := c.transport.Type()

	c.broker.mu.RLock()
	subClient, ok := c.broker.clients[subID]
	c.broker.mu.RUnlock()

	if ok {
		log.Printf("[%s] 메시지 전달: from=%s, to=%s, topic=%s",
			transportType, formatClientID(publisherID), formatClientID(subID), topic)

		if !subClient.SendPublish(topic, payload) {
			log.Printf("[%s] 구독자 연결 끊김, 제거: id=%s, topic=%s",
				transportType, formatClientID(subID), topic)
			c.removeClientAndSubscriptions(subID, topic)
		}
	} else {
		log.Printf("[%s] 구독자 오프라인, 제거: id=%s, topic=%s",
			transportType, formatClientID(subID), topic)
		c.broker.store.RemoveSubscriber(topic, subID)
	}
}

func (c *BaseClient) removeClientAndSubscriptions(clientID, topic string) {
	c.broker.mu.Lock()
	delete(c.broker.clients, clientID)
	c.broker.mu.Unlock()
	c.broker.store.RemoveSubscriber(topic, clientID)
}

func formatClientID(id string) string {
	if id == "" {
		return "(없음)"
	}
	return id
}
