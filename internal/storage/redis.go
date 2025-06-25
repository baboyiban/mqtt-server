package storage

import (
	"context"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"
)

type Store interface {
	AddSubscriber(topic, clientID string) error
	GetSubscribers(topic string) []string
	Ping() error
	RemoveSubscriber(topic, clientID string) error
}

type RedisStore struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisStore(addr string) *RedisStore {
	return &RedisStore{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		ctx:    context.Background(),
	}
}

func (r *RedisStore) AddSubscriber(topic, clientID string) error {
	key := "subscribers:" + topic
	err := r.client.SAdd(r.ctx, key, clientID).Err()
	if err != nil {
		log.Printf("[Redis] 구독 등록 실패: topic=%s, id=%s, err=%v", topic, clientID, err)
	}
	return err
}

func (r *RedisStore) GetSubscribers(topic string) []string {
	key := "subscribers:" + topic
	subs, err := r.client.SMembers(r.ctx, key).Result()
	if err != nil {
		log.Printf("[Redis] 구독자 조회 실패: topic=%s, err=%v", topic, err)
		return nil
	}
	if len(subs) > 0 {
		log.Printf("[Redis] 구독자 목록: topic=%s, subs=%s", topic, strings.Join(subs, ","))
	}
	return subs
}

func (r *RedisStore) Ping() error {
	return r.client.Ping(r.ctx).Err()
}

func (r *RedisStore) RemoveSubscriber(topic, clientID string) error {
	key := "subscribers:" + topic
	err := r.client.SRem(r.ctx, key, clientID).Err()
	if err != nil {
		log.Printf("[Redis] 구독 삭제 실패: topic=%s, id=%s, err=%v", topic, clientID, err)
	}
	return err
}
