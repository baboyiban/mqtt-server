package broker

import (
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/baboyiban/mqtt-server/internal/storage"
	"github.com/gorilla/websocket"
)

// BrokerConfig는 MQTT Broker 설정을 담고 있습니다
type BrokerConfig struct {
	AllowAnonymous   bool     // 익명 클라이언트 허용 여부
	AllowEmptyClient bool     // 빈 클라이언트 ID 허용 여부
	MaxClients       int      // 최대 동시 연결 수 (0은 무제한)
	EnableWebSocket  bool     // WebSocket 서버 활성화 여부
	CORSOrigins      []string // 허용할 CORS Origin 목록 (빈 배열은 모두 허용)
}

// Broker는 MQTT 브로커의 핵심 구현체입니다
type Broker struct {
	store   storage.Store
	clients map[string]Clienter
	mu      sync.RWMutex
	config  BrokerConfig
}

// NewBroker는 새 브로커 인스턴스를 생성합니다
func NewBroker(store storage.Store) *Broker {
	return &Broker{
		store:   store,
		clients: make(map[string]Clienter),
		config: BrokerConfig{
			AllowAnonymous:   true,
			AllowEmptyClient: true,
			MaxClients:       0,
			EnableWebSocket:  true,
			CORSOrigins:      []string{"*"},
		},
	}
}

// SetConfig는 브로커 설정을 업데이트합니다
func (b *Broker) SetConfig(config BrokerConfig) {
	b.config = config
}

// GetClientCount는 현재 연결된 클라이언트 수를 반환합니다
func (b *Broker) GetClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// ListenAndServe는 MQTT TCP 서버를 시작합니다
func (b *Broker) ListenAndServe(addr string) error {
	// TCP 리스너 설정
	ln, err := b.setupTCPListener(addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	// 연결 수락 루프
	return b.acceptTCPConnections(ln)
}

// setupTCPListener는 TCP 리스너를 설정합니다
func (b *Broker) setupTCPListener(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	log.Printf("[서버] MQTT TCP 서버 시작: addr=%s", addr)
	return ln, nil
}

// acceptTCPConnections는 TCP 연결 요청을 처리합니다
func (b *Broker) acceptTCPConnections(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[서버] TCP 연결 실패: err=%v", err)
			continue
		}

		// 연결 요청 처리
		b.handleTCPConnection(conn)
	}
}

// handleTCPConnection은 새로운 TCP 연결을 처리합니다
func (b *Broker) handleTCPConnection(conn net.Conn) {
	// 연결 한도 확인
	if !b.checkConnectionLimit(conn) {
		conn.Close()
		return
	}

	// 클라이언트 처리 시작
	log.Printf("[서버] TCP 연결 수락: addr=%s", conn.RemoteAddr())
	go b.handleClient(conn)
}

// checkConnectionLimit는 최대 연결 수 제한을 확인합니다
func (b *Broker) checkConnectionLimit(conn net.Conn) bool {
	if b.config.MaxClients > 0 && b.GetClientCount() >= b.config.MaxClients {
		log.Printf("[서버] 최대 클라이언트 수 초과, 연결 거부: addr=%s, count=%d/%d",
			conn.RemoteAddr(), b.GetClientCount(), b.config.MaxClients)
		return false
	}
	return true
}

// ListenAndServeWS는 MQTT WebSocket 서버를 시작합니다
func (b *Broker) ListenAndServeWS(addr string) error {
	// WebSocket이 비활성화된 경우
	if !b.config.EnableWebSocket {
		log.Printf("[서버] WebSocket 서버 비활성화됨")
		select {} // 비활성화된 경우 블록
	}

	// HTTP 핸들러 등록
	b.setupWebSocketHandlers()

	// WebSocket 서버 시작
	log.Printf("[서버] MQTT WebSocket 서버 시작: addr=%s", addr)
	return http.ListenAndServe(addr, nil)
}

// setupWebSocketHandlers는 WebSocket HTTP 핸들러를 설정합니다
func (b *Broker) setupWebSocketHandlers() {
	// MQTT WebSocket 전용 경로
	http.HandleFunc("/mqtt", func(w http.ResponseWriter, r *http.Request) {
		b.handleWebSocketRequest(w, r)
	})

	// 기본 경로 핸들러 (호환성을 위해 유지)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		b.handleRootPath(w, r)
	})
}

// handleRootPath는 루트 경로 요청을 처리합니다
func (b *Broker) handleRootPath(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		w.Write([]byte("MQTT WebSocket 서버가 실행 중입니다. /mqtt 경로로 연결하세요."))
		return
	}
	// 다른 경로는 WebSocket 요청으로 처리
	b.handleWebSocketRequest(w, r)
}

// handleWebSocketRequest는 WebSocket 연결 요청을 처리합니다
func (b *Broker) handleWebSocketRequest(w http.ResponseWriter, r *http.Request) {
	// CORS 설정
	b.setupCORSHeaders(w, r)

	// OPTIONS 요청 처리
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// 연결 한도 확인
	if !b.checkWSConnectionLimit(w, r) {
		return
	}

	// WebSocket 연결 수립
	b.upgradeAndHandleWSConnection(w, r)
}

// setupCORSHeaders는 CORS 헤더를 설정합니다
func (b *Broker) setupCORSHeaders(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" || len(b.config.CORSOrigins) == 0 {
		return
	}

	// 허용된 오리진인지 확인
	allowed := b.isOriginAllowed(origin)
	if allowed {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	}
}

// isOriginAllowed는 주어진 오리진이 허용된 오리진인지 확인합니다
func (b *Broker) isOriginAllowed(origin string) bool {
	for _, allowedOrigin := range b.config.CORSOrigins {
		if allowedOrigin == "*" || allowedOrigin == origin {
			return true
		}
	}
	return false
}

// checkWSConnectionLimit는 WebSocket 연결 한도를 확인합니다
func (b *Broker) checkWSConnectionLimit(w http.ResponseWriter, r *http.Request) bool {
	if b.config.MaxClients > 0 && b.GetClientCount() >= b.config.MaxClients {
		log.Printf("[서버] 최대 클라이언트 수 초과, WebSocket 연결 거부: addr=%s, count=%d/%d",
			r.RemoteAddr, b.GetClientCount(), b.config.MaxClients)
		http.Error(w, "Too many connections", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// upgradeAndHandleWSConnection은 HTTP 연결을 WebSocket으로 업그레이드하고 처리합니다
func (b *Broker) upgradeAndHandleWSConnection(w http.ResponseWriter, r *http.Request) {
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[서버] WebSocket 업그레이드 실패: addr=%s, err=%v", r.RemoteAddr, err)
		return
	}

	log.Printf("[서버] WebSocket 연결 수락: addr=%s", wsConn.RemoteAddr())
	go b.handleWSClient(wsConn)
}

// WebSocket 업그레이더 설정
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		log.Printf("WebSocket Origin: %s", r.Header.Get("Origin"))
		return true // 모든 오리진 허용 (CORS는 handleWebSocketRequest에서 처리)
	},
	Subprotocols: []string{"mqtt", "mqttv3.1"},
}

// handleClient는 새 TCP 클라이언트 연결을 처리합니다
func (b *Broker) handleClient(conn net.Conn) {
	client := NewClient(conn, b)
	client.Handle()
}

// handleWSClient는 새 WebSocket 클라이언트 연결을 처리합니다
func (b *Broker) handleWSClient(wsConn *websocket.Conn) {
	client := NewWSClient(wsConn, b)
	client.Handle()
}

// GetStore는 브로커의 스토리지 인스턴스를 반환합니다
func (b *Broker) GetStore() storage.Store {
	return b.store
}

// BroadcastToTopic은 특정 토픽에 메시지를 발행합니다 (시스템 메시지용)
func (b *Broker) BroadcastToTopic(topic string, payload []byte) {
	subs := b.store.GetSubscribers(topic)
	if len(subs) == 0 {
		log.Printf("[브로커] 구독자 없음: topic=%s", topic)
		return
	}

	// 브로드캐스트 처리
	b.broadcastToSubscribers(topic, payload, subs)
}

// broadcastToSubscribers는 모든 구독자에게 메시지를 전송합니다
func (b *Broker) broadcastToSubscribers(topic string, payload []byte, subscribers []string) {
	for _, subID := range subscribers {
		b.sendMessageToSubscriber(topic, payload, subID)
	}
}

// sendMessageToSubscriber는 단일 구독자에게 메시지를 전송합니다
func (b *Broker) sendMessageToSubscriber(topic string, payload []byte, subscriberID string) {
	// 구독자 클라이언트 확인
	b.mu.RLock()
	subClient, ok := b.clients[subscriberID]
	b.mu.RUnlock()

	if ok {
		// 메시지 전송 시도
		if !subClient.SendPublish(topic, payload, false) {
			// 전송 실패 시 구독자 제거
			b.removeSubscriber(topic, subscriberID)
		}
	} else {
		// 클라이언트가 없는 경우 구독 정보만 제거
		b.store.RemoveSubscriber(topic, subscriberID)
	}
}

// removeSubscriber는 구독자를 제거합니다
func (b *Broker) removeSubscriber(topic string, subscriberID string) {
	b.mu.Lock()
	delete(b.clients, subscriberID)
	b.mu.Unlock()
	b.store.RemoveSubscriber(topic, subscriberID)
}
