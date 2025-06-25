package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/baboyiban/mqtt-server/internal/mqtt"
	"github.com/redis/go-redis/v9"
)

// Store 인터페이스는 MQTT 브로커의 영구 저장소를 정의합니다
type Store interface {
	// 구독 관리
	AddSubscriber(topic, clientID string) error
	RemoveSubscriber(topic, clientID string) error
	GetSubscribers(topic string) []string

	// 시스템 관리
	Ping() error
	Close() error
	Cleanup() error

	// 메시지 저장소 (옵션)
	StoreRetainedMessage(topic string, payload []byte) error
	GetRetainedMessage(topic string) ([]byte, error)
	GetMatchingRetainedMessages(filter string) ([]*mqtt.PublishPacket, error)
	RemoveRetainedMessage(topic string) error

	// QoS 2 In-flight 메시지 관리
	StoreInflightMessage(clientID string, packet *mqtt.PublishPacket) error
	RemoveInflightMessage(clientID string, packetID uint16) error
	GetAllInflightMessages(clientID string) (map[uint16]*mqtt.PublishPacket, error)
}

// RedisConfig는 Redis 연결 설정을 담습니다
type RedisConfig struct {
	KeyPrefix  string        // Redis 키 접두사
	Expiration time.Duration // 키 만료 시간 (0은 만료 없음)
}

// RedisStore는 Redis를 사용한 Store 구현체입니다
type RedisStore struct {
	client *redis.Client
	ctx    context.Context
	config RedisConfig
}

// for JSON serialization
type inflightPayload struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
}

// NewRedisStore는 Redis 스토어 인스턴스를 생성합니다
func NewRedisStore(addr, password string, db int) *RedisStore {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &RedisStore{
		client: client,
		ctx:    context.Background(),
		config: RedisConfig{
			KeyPrefix:  "mqtt:",
			Expiration: 0, // 기본적으로 만료 없음
		},
	}
}

// SetConfig는 Redis 스토어 설정을 업데이트합니다
func (r *RedisStore) SetConfig(config RedisConfig) {
	r.config = config
}

// AddSubscriber는 토픽에 구독자를 추가합니다
func (r *RedisStore) AddSubscriber(topic, clientID string) error {
	key := r.getSubscriberKey(topic)
	err := r.client.SAdd(r.ctx, key, clientID).Err()
	if err != nil {
		log.Printf("[Redis] 구독 등록 실패: topic=%s, id=%s, err=%v", topic, clientID, err)
		return fmt.Errorf("구독 등록 실패: %w", err)
	}

	// 만료 시간 설정 (만료 시간이 있는 경우)
	if r.config.Expiration > 0 {
		r.client.Expire(r.ctx, key, r.config.Expiration)
	}

	return nil
}

// RemoveSubscriber는 토픽에서 구독자를 제거합니다
func (r *RedisStore) RemoveSubscriber(topic, clientID string) error {
	key := r.getSubscriberKey(topic)
	err := r.client.SRem(r.ctx, key, clientID).Err()
	if err != nil {
		log.Printf("[Redis] 구독 삭제 실패: topic=%s, id=%s, err=%v", topic, clientID, err)
		return fmt.Errorf("구독 삭제 실패: %w", err)
	}

	// 구독자가 없으면 키 삭제
	count, err := r.client.SCard(r.ctx, key).Result()
	if err == nil && count == 0 {
		r.client.Del(r.ctx, key)
	}

	return nil
}

// GetSubscribers는 특정 토픽에 대한 모든 구독자 (와일드카드 포함)를 반환합니다.
func (r *RedisStore) GetSubscribers(topic string) []string {
	parts := strings.Split(topic, "/")
	keys := []string{r.getSubscriberKey(topic), r.getSubscriberKey("#")}

	for i := range parts {
		// 멀티 레벨 와일드카드 (#) 키 생성
		if i < len(parts) {
			prefix := strings.Join(parts[:i], "/")
			if prefix != "" {
				prefix += "/"
			}
			keys = append(keys, r.getSubscriberKey(prefix+"#"))
		}

		// 싱글 레벨 와일드카드 (+) 키 생성
		tempParts := make([]string, len(parts))
		copy(tempParts, parts)
		tempParts[i] = "+"
		keys = append(keys, r.getSubscriberKey(strings.Join(tempParts, "/")))
	}

	// 중복 제거
	keyMap := make(map[string]struct{})
	uniqueKeys := []string{}
	for _, k := range keys {
		if _, exists := keyMap[k]; !exists {
			keyMap[k] = struct{}{}
			uniqueKeys = append(uniqueKeys, k)
		}
	}

	if len(uniqueKeys) == 0 {
		return nil
	}

	subs, err := r.client.SUnion(r.ctx, uniqueKeys...).Result()
	if err != nil {
		if err != redis.Nil {
			log.Printf("[Redis] 구독자 조회 실패 (SUnion): topic=%s, err=%v", topic, err)
		}
		return nil
	}

	return subs
}

// StoreRetainedMessage는 지정된 토픽에 대한 메시지를 저장합니다
func (r *RedisStore) StoreRetainedMessage(topic string, payload []byte) error {
	key := r.getRetainedKey(topic)
	err := r.client.Set(r.ctx, key, payload, r.config.Expiration).Err()
	if err != nil {
		log.Printf("[Redis] Retained 메시지 저장 실패: topic=%s, err=%v", topic, err)
		return fmt.Errorf("메시지 저장 실패: %w", err)
	}
	return nil
}

// GetRetainedMessage는 지정된 토픽에 대한 retained 메시지를 반환합니다
func (r *RedisStore) GetRetainedMessage(topic string) ([]byte, error) {
	key := r.getRetainedKey(topic)
	val, err := r.client.Get(r.ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			// 메시지가 없는 경우 (에러 아님)
			return nil, nil
		}
		log.Printf("[Redis] Retained 메시지 조회 실패: topic=%s, err=%v", topic, err)
		return nil, fmt.Errorf("메시지 조회 실패: %w", err)
	}
	return val, nil
}

// GetMatchingRetainedMessages는 와일드카드 필터와 일치하는 모든 보존 메시지를 반환합니다.
func (r *RedisStore) GetMatchingRetainedMessages(filter string) ([]*mqtt.PublishPacket, error) {
	var messages []*mqtt.PublishPacket
	iter := r.client.Scan(r.ctx, 0, r.getRetainedKey("*"), 0).Iterator()

	for iter.Next(r.ctx) {
		key := iter.Val()
		retainedTopic := strings.TrimPrefix(key, r.config.KeyPrefix+"retained:")

		if Match(filter, retainedTopic) {
			payload, err := r.client.Get(r.ctx, key).Bytes()
			if err != nil {
				log.Printf("[Redis] 보존 메시지 페이로드 조회 실패: key=%s, err=%v", key, err)
				continue
			}
			messages = append(messages, &mqtt.PublishPacket{
				Topic:   retainedTopic,
				Payload: payload,
				Retain:  true,
			})
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("retained 메시지 스캔 실패: %w", err)
	}
	return messages, nil
}

// RemoveRetainedMessage는 지정된 토픽의 retained 메시지를 제거합니다
func (r *RedisStore) RemoveRetainedMessage(topic string) error {
	key := r.getRetainedKey(topic)
	err := r.client.Del(r.ctx, key).Err()
	if err != nil {
		log.Printf("[Redis] Retained 메시지 삭제 실패: topic=%s, err=%v", topic, err)
		return fmt.Errorf("메시지 삭제 실패: %w", err)
	}
	return nil
}

// --- In-flight message methods ---

// StoreInflightMessage는 핸드셰이크 중인 QoS 2 메시지를 저장합니다.
func (r *RedisStore) StoreInflightMessage(clientID string, packet *mqtt.PublishPacket) error {
	key := r.getInflightKey(clientID)
	field := strconv.Itoa(int(packet.PacketID))

	payload, err := json.Marshal(inflightPayload{
		Topic:   packet.Topic,
		Payload: packet.Payload,
	})
	if err != nil {
		return fmt.Errorf("inflight 메시지 직렬화 실패: %w", err)
	}

	if err := r.client.HSet(r.ctx, key, field, payload).Err(); err != nil {
		return fmt.Errorf("inflight 메시지 저장 실패: %w", err)
	}
	return nil
}

// RemoveInflightMessage는 핸드셰이크가 완료된 메시지를 저장소에서 삭제합니다.
func (r *RedisStore) RemoveInflightMessage(clientID string, packetID uint16) error {
	key := r.getInflightKey(clientID)
	field := strconv.Itoa(int(packetID))
	if err := r.client.HDel(r.ctx, key, field).Err(); err != nil {
		return fmt.Errorf("inflight 메시지 삭제 실패: %w", err)
	}
	return nil
}

// GetAllInflightMessages는 클라이언트의 모든 in-flight 메시지를 가져옵니다. (서버 재시작 시 사용)
func (r *RedisStore) GetAllInflightMessages(clientID string) (map[uint16]*mqtt.PublishPacket, error) {
	key := r.getInflightKey(clientID)
	res, err := r.client.HGetAll(r.ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("모든 inflight 메시지 조회 실패: %w", err)
	}

	messages := make(map[uint16]*mqtt.PublishPacket)
	for field, val := range res {
		packetIDInt, err := strconv.Atoi(field)
		if err != nil {
			log.Printf("[Redis] 잘못된 packetID 필드 발견: key=%s, field=%s", key, field)
			continue
		}
		packetID := uint16(packetIDInt)

		var payload inflightPayload
		if err := json.Unmarshal([]byte(val), &payload); err != nil {
			log.Printf("[Redis] 잘못된 inflight 메시지 payload 발견: key=%s, field=%s, err=%v", key, field, err)
			continue
		}

		messages[packetID] = &mqtt.PublishPacket{
			PacketID: packetID,
			Topic:    payload.Topic,
			Payload:  payload.Payload,
		}
	}

	return messages, nil
}

// Ping은 Redis 서버 연결을 확인합니다
func (r *RedisStore) Ping() error {
	return r.client.Ping(r.ctx).Err()
}

// Close는 Redis 연결을 종료합니다
func (r *RedisStore) Close() error {
	return r.client.Close()
}

// Cleanup은 만료된 키들을 정리합니다
func (r *RedisStore) Cleanup() error {
	// 필요한 경우 정리 로직 구현
	return nil
}

// 키 생성 헬퍼 함수들
func (r *RedisStore) getSubscriberKey(topic string) string {
	return r.config.KeyPrefix + "subscribers:" + topic
}

func (r *RedisStore) getRetainedKey(topic string) string {
	return r.config.KeyPrefix + "retained:" + topic
}

func (r *RedisStore) getInflightKey(clientID string) string {
	return r.config.KeyPrefix + "inflight:" + clientID
}

// Match는 MQTT 토픽 필터가 주어진 토픽과 일치하는지 확인합니다.
func Match(filter, topic string) bool {
	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topic, "/")

	fLen := len(filterParts)
	tLen := len(topicParts)

	for i := 0; i < fLen; i++ {
		if i >= tLen {
			// 필터가 더 길지만 마지막 문자가 #가 아니면 불일치
			return filterParts[i] == "#" && i == fLen-1
		}

		fPart := filterParts[i]
		tPart := topicParts[i]

		if fPart == "#" {
			// #는 마지막이어야 함
			return i == fLen-1
		}

		if fPart != "+" && fPart != tPart {
			return false
		}
	}

	// 필터와 토픽의 길이가 같으면 일치
	return fLen == tLen
}
