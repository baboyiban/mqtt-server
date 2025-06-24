package storage

import (
	"context"
	"log"

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
	return r.client.SAdd(r.ctx, "subscribers:"+topic, clientID).Err()
}

func (r *RedisStore) GetSubscribers(topic string) []string {
	key := "subscribers:" + topic
	subs, err := r.client.SMembers(r.ctx, key).Result()
	if err != nil {
		log.Printf("Redis 구독자 조회 실패: %v", err)
		return nil
	}
	log.Printf("Redis 구독자 조회 [%s]: %v", key, subs)
	return subs
}

func (r *RedisStore) Ping() error {
	return r.client.Ping(r.ctx).Err()
}

func (r *RedisStore) RemoveSubscriber(topic, clientID string) error {
	return r.client.SRem(r.ctx, "subscribers:"+topic, clientID).Err()
}
