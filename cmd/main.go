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

	// 평문 TCP 1883
	go func() {
		if err := b.ListenAndServe(":1883"); err != nil {
			log.Fatalf("MQTT 서버 종료: %v", err)
		}
	}()

	// 평문 WS 8083
	go func() {
		if err := b.ListenAndServeWS(":8083"); err != nil {
			log.Fatalf("MQTT WS 서버 종료: %v", err)
		}
	}()

	select {}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
