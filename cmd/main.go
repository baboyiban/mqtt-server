package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/baboyiban/mqtt-server/internal/broker"
	"github.com/baboyiban/mqtt-server/internal/storage"
)

// Config 구조체는 애플리케이션 설정을 담습니다
type Config struct {
	TCPPort          int
	WSPort           int
	EnableTCP        bool
	EnableWS         bool
	MaxClients       int
	CORSOrigins      []string
	RedisAddr        string
	RedisPassword    string
	RedisDB          int
	AllowAnonymous   bool
	AllowEmptyClient bool
}

func main() {
	config := loadConfigFromEnv()
	printConfig(config)

	store := mustInitStore(config)
	b := setupBroker(config, store)

	startServers(config, b)

	log.Printf("[시작] MQTT 서버 준비 완료")
	waitForShutdown()
}

// --- Config 관련 함수 ---

func loadConfigFromEnv() Config {
	return Config{
		TCPPort:          getEnvInt("TCP_PORT", 1883),
		WSPort:           getEnvInt("WS_PORT", 8083),
		EnableTCP:        getEnvBool("ENABLE_TCP", true),
		EnableWS:         getEnvBool("ENABLE_WS", true),
		MaxClients:       getEnvInt("MAX_CLIENTS", 0),
		CORSOrigins:      getEnvStringSlice("CORS_ORIGINS", []string{"*"}),
		RedisAddr:        getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    getEnv("REDIS_PASSWORD", ""),
		RedisDB:          getEnvInt("REDIS_DB", 0),
		AllowAnonymous:   getEnvBool("ALLOW_ANONYMOUS", true),
		AllowEmptyClient: getEnvBool("ALLOW_EMPTY_CLIENT", true),
	}
}

func printConfig(cfg Config) {
	log.Printf("[설정] TCP 서버: 활성화=%v, 포트=%d", cfg.EnableTCP, cfg.TCPPort)
	log.Printf("[설정] WS 서버: 활성화=%v, 포트=%d", cfg.EnableWS, cfg.WSPort)
	log.Printf("[설정] 최대 클라이언트 수: %d (0=무제한)", cfg.MaxClients)
	log.Printf("[설정] CORS 허용 Origin: %v", cfg.CORSOrigins)
	log.Printf("[설정] Redis 연결: addr=%s, db=%d", cfg.RedisAddr, cfg.RedisDB)
	log.Printf("[설정] 브로커: 익명허용=%v, 빈ID허용=%v", cfg.AllowAnonymous, cfg.AllowEmptyClient)
}

// --- Store & Broker 관련 함수 ---

func mustInitStore(cfg Config) storage.Store {
	log.Printf("[시작] Redis 연결 시도: addr=%s", cfg.RedisAddr)
	store := storage.NewRedisStore(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err := store.Ping(); err != nil {
		log.Fatalf("[오류] Redis 연결 실패: addr=%s, err=%v", cfg.RedisAddr, err)
	}
	log.Printf("[시작] Redis 연결 성공: addr=%s", cfg.RedisAddr)
	return store
}

func setupBroker(cfg Config, store storage.Store) *broker.Broker {
	b := broker.NewBroker(store)
	b.SetConfig(broker.BrokerConfig{
		AllowAnonymous:   cfg.AllowAnonymous,
		AllowEmptyClient: cfg.AllowEmptyClient,
		MaxClients:       cfg.MaxClients,
		EnableWebSocket:  cfg.EnableWS,
		CORSOrigins:      cfg.CORSOrigins,
	})
	return b
}

// --- 서버 시작 함수 ---

func startServers(cfg Config, b *broker.Broker) {
	if cfg.EnableTCP {
		go func() {
			addr := fmt.Sprintf(":%d", cfg.TCPPort)
			log.Printf("[시작] MQTT TCP 서버 시작 중: addr=%s", addr)
			if err := b.ListenAndServe(addr); err != nil {
				log.Fatalf("[오류] MQTT TCP 서버 종료: err=%v", err)
			}
		}()
	}
	if cfg.EnableWS {
		go func() {
			addr := fmt.Sprintf(":%d", cfg.WSPort)
			log.Printf("[시작] MQTT WebSocket 서버 시작 중: addr=%s", addr)
			if err := b.ListenAndServeWS(addr); err != nil {
				log.Fatalf("[오류] MQTT WebSocket 서버 종료: err=%v", err)
			}
		}()
	}
}

// --- 종료 시그널 처리 ---

func waitForShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	log.Printf("[종료] 시그널 %v 수신, 서버 종료 중...", sig)
	log.Printf("[종료] 서버가 정상적으로 종료되었습니다.")
	os.Exit(0)
}

// --- 환경변수 유틸리티 ---

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	strValue := os.Getenv(key)
	if strValue == "" {
		return fallback
	}
	value, err := strconv.Atoi(strValue)
	if err != nil {
		log.Printf("[경고] 환경변수 %s의 값 %s이(가) 정수가 아님, 기본값 %d 사용", key, strValue, fallback)
		return fallback
	}
	return value
}

func getEnvBool(key string, fallback bool) bool {
	strValue := os.Getenv(key)
	if strValue == "" {
		return fallback
	}
	value, err := strconv.ParseBool(strValue)
	if err != nil {
		log.Printf("[경고] 환경변수 %s의 값 %s이(가) 불리언이 아님, 기본값 %v 사용", key, strValue, fallback)
		return fallback
	}
	return value
}

func getEnvStringSlice(key string, fallback []string) []string {
	strValue := os.Getenv(key)
	if strValue == "" {
		return fallback
	}
	return strings.Split(strValue, ",")
}
