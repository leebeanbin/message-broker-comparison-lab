// Package config는 환경변수 기반 설정 관리를 담당합니다.
// 모든 설정값은 환경변수에서 읽으며, 기본값이 제공됩니다.
package config

import (
	"os"
	"strconv"
)

// Config는 이미지 처리 서비스의 전체 설정을 담는 구조체입니다.
type Config struct {
	// RabbitMQ 연결 설정
	RabbitMQHost string
	RabbitMQPort int
	RabbitMQUser string
	RabbitMQPass string

	// Redis 연결 설정
	RedisHost string
	RedisPort int

	// HTTP 서버 설정
	HTTPPort int

	// 큐 이름 설정
	InputQueue  string // 이미지 처리 요청을 받는 큐
	ResultQueue string // 처리 완료 결과를 보내는 큐
}

// Load는 환경변수에서 설정을 읽어 Config를 반환합니다.
// 환경변수가 없으면 기본값을 사용합니다.
func Load() *Config {
	return &Config{
		RabbitMQHost: getEnv("RABBITMQ_HOST", "localhost"),
		RabbitMQPort: getEnvInt("RABBITMQ_PORT", 5672),
		RabbitMQUser: getEnv("RABBITMQ_USER", "guest"),
		RabbitMQPass: getEnv("RABBITMQ_PASS", "guest"),

		RedisHost: getEnv("REDIS_HOST", "localhost"),
		RedisPort: getEnvInt("REDIS_PORT", 6379),

		HTTPPort: getEnvInt("HTTP_PORT", 8081),

		InputQueue:  getEnv("INPUT_QUEUE", "image-processing-queue"),
		ResultQueue: getEnv("RESULT_QUEUE", "image-result-queue"),
	}
}

// RabbitMQURL은 AMQP 연결 URL을 생성합니다.
func (c *Config) RabbitMQURL() string {
	return "amqp://" + c.RabbitMQUser + ":" + c.RabbitMQPass + "@" +
		c.RabbitMQHost + ":" + strconv.Itoa(c.RabbitMQPort) + "/"
}

// RedisAddr은 Redis 연결 주소를 생성합니다.
func (c *Config) RedisAddr() string {
	return c.RedisHost + ":" + strconv.Itoa(c.RedisPort)
}

// getEnv는 환경변수를 읽고, 없으면 기본값을 반환합니다.
func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// getEnvInt는 환경변수를 정수로 읽고, 없거나 파싱 실패 시 기본값을 반환합니다.
func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return defaultVal
}
