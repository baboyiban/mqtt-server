package main

import (
	"log"
	"os"

	"github.com/baboyiban/mqtt-server/internal/broker"
	"github.com/baboyiban/mqtt-server/internal/storage"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	log.Printf("[시작] Redis 연결 시도: addr=%s", redisAddr)

	store := storage.NewRedisStore(redisAddr)
	if err := store.Ping(); err != nil {
		log.Fatalf("[오류] Redis 연결 실패: addr=%s, err=%v", redisAddr, err)
	}
	log.Printf("[시작] Redis 연결 성공: addr=%s", redisAddr)

	b := broker.NewBroker(store)

	// 평문 TCP 1883
	go func() {
		if err := b.ListenAndServe(":1883"); err != nil {
			log.Fatalf("[오류] MQTT TCP 서버 종료: err=%v", err)
		}
	}()

	// 평문 WS 8083
	go func() {
		if err := b.ListenAndServeWS(":8083"); err != nil {
			log.Fatalf("[오류] MQTT WebSocket 서버 종료: err=%v", err)
		}
	}()

	log.Printf("[시작] MQTT 서버 준비 완료")
	select {}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
