package main

import (
	"log"
	"os"

	"github.com/baboyiban/mqtt-server/internal/broker"
	"github.com/baboyiban/mqtt-server/internal/storage"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	store := storage.NewRedisStore(redisAddr)
	if err := store.Ping(); err != nil {
		log.Fatalf("Redis 연결 실패: %v", err)
	}
	log.Println("Redis 연결 성공")

	b := broker.NewBroker(store)
	if err := b.ListenAndServe(":1883"); err != nil {
		log.Fatalf("MQTT 서버 종료: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
