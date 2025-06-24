package storage

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Store interface {
	AddSubscriber(topic, clientID string) error
	GetSubscribers(topic string) []string
	Ping() error
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
	subs, err := r.client.SMembers(r.ctx, "subscribers:"+topic).Result()
	if err != nil {
		return nil
	}
	return subs
}

func (r *RedisStore) Ping() error {
	return r.client.Ping(r.ctx).Err()
}
