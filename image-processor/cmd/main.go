// 이미지 처리 마이크로서비스 엔트리포인트
//
// 역할:
//   1. RabbitMQ 'image-processing-queue'에서 이미지 처리 요청 수신
//   2. 이미지 다운로드 → 리사이즈 → 필터 적용 → 인코딩
//   3. 결과를 Redis에 저장 (image:result:{task_id})
//   4. 완료 이벤트를 RabbitMQ 'image-result-queue'에 발행
//   5. /health 엔드포인트 (port 8081)
//   6. /metrics Prometheus 메트릭 노출

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/lab/image-processor/internal/config"
	"github.com/lab/image-processor/internal/handler"
)

func main() {
	log.Println("🚀 이미지 처리 서비스 시작...")

	// ── 설정 로드 ──
	cfg := config.Load()
	log.Printf("📋 설정: RabbitMQ=%s:%d, Redis=%s:%d, HTTP=:%d",
		cfg.RabbitMQHost, cfg.RabbitMQPort, cfg.RedisHost, cfg.RedisPort, cfg.HTTPPort)

	// ── Redis 연결 ──
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr(),
	})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Redis 연결 실패: %v", err)
	}
	log.Println("✅ Redis 연결 성공")

	// ── RabbitMQ 연결 (재시도 포함) ──
	var conn *amqp.Connection
	var err error
	for i := 0; i < 30; i++ {
		conn, err = amqp.Dial(cfg.RabbitMQURL())
		if err == nil {
			break
		}
		log.Printf("⏳ RabbitMQ 연결 대기 중... (%d/30): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("❌ RabbitMQ 연결 실패: %v", err)
	}
	defer conn.Close()
	log.Println("✅ RabbitMQ 연결 성공")

	// ── 채널 생성 ──
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ RabbitMQ 채널 생성 실패: %v", err)
	}
	defer ch.Close()

	// ── 큐 선언 (입력 큐 + 결과 큐) ──
	_, err = ch.QueueDeclare(cfg.InputQueue, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ 입력 큐 선언 실패: %v", err)
	}

	_, err = ch.QueueDeclare(cfg.ResultQueue, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ 결과 큐 선언 실패: %v", err)
	}

	// ── QoS 설정: 동시 처리 5개 제한 ──
	err = ch.Qos(5, 0, false)
	if err != nil {
		log.Fatalf("❌ QoS 설정 실패: %v", err)
	}

	// ── 이미지 핸들러 생성 ──
	imgHandler := handler.NewImageHandler(rdb)

	// ── Consumer 시작 ──
	msgs, err := ch.Consume(cfg.InputQueue, "image-processor", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ Consumer 시작 실패: %v", err)
	}
	log.Printf("👂 '%s' 큐에서 메시지 대기 중...", cfg.InputQueue)

	// ── HTTP 서버 (health + metrics) ──
	mux := http.NewServeMux()

	// /health 엔드포인트: 서비스 상태 확인
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		// Redis, RabbitMQ 연결 상태 확인
		status := map[string]string{
			"service":  "image-processor",
			"status":   "healthy",
			"redis":    "connected",
			"rabbitmq": "connected",
		}

		if rdb.Ping(ctx).Err() != nil {
			status["redis"] = "disconnected"
			status["status"] = "degraded"
		}
		if conn.IsClosed() {
			status["rabbitmq"] = "disconnected"
			status["status"] = "degraded"
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	// /metrics 엔드포인트: Prometheus 메트릭
	mux.Handle("/metrics", promhttp.Handler())

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: mux,
	}

	go func() {
		log.Printf("🌐 HTTP 서버 시작 (:%d) - /health, /metrics", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("❌ HTTP 서버 에러: %v", err)
		}
	}()

	// ── 메시지 처리 고루틴 ──
	done := make(chan struct{})
	go func() {
		for msg := range msgs {
			processMessage(ctx, ch, imgHandler, cfg.ResultQueue, msg)
		}
		close(done)
	}()

	// ── Graceful Shutdown ──
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("🛑 시그널 수신: %v, 종료 중...", sig)

	// HTTP 서버 종료
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	httpServer.Shutdown(shutdownCtx)

	// RabbitMQ 채널 닫기 → Consumer 루프 종료
	ch.Close()
	<-done

	log.Println("👋 이미지 처리 서비스 종료 완료")
}

// processMessage는 단일 메시지를 처리합니다.
// 처리 흐름: 메시지 파싱 → 이미지 처리 → 결과 발행 → ACK
func processMessage(
	ctx context.Context,
	ch *amqp.Channel,
	imgHandler *handler.ImageHandler,
	resultQueue string,
	msg amqp.Delivery,
) {
	var req handler.ProcessRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		log.Printf("❌ 메시지 파싱 실패: %v", err)
		msg.Nack(false, false) // DLQ로 보내기 (requeue하지 않음)
		return
	}

	log.Printf("📥 처리 시작: %s (URL: %s, %dx%d → %dx%d)",
		req.TaskID, req.ImageURL,
		0, 0, req.TargetWidth, req.TargetHeight)

	// 이미지 처리 실행
	result, err := imgHandler.Process(ctx, &req)
	if err != nil {
		log.Printf("❌ 처리 실패 [%s]: %v", req.TaskID, err)
		// 실패 결과도 발행 (Python에서 실패 알 수 있도록)
		result = &handler.ProcessResult{
			TaskID: req.TaskID,
			Status: "failed",
			Error:  err.Error(),
		}
	}

	// 결과를 RabbitMQ에 발행 (Python이 수신)
	resultJSON, _ := json.Marshal(result)
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(pubCtx, "", resultQueue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         resultJSON,
		DeliveryMode: amqp.Persistent,
	})
	if err != nil {
		log.Printf("❌ 결과 발행 실패 [%s]: %v", req.TaskID, err)
		msg.Nack(false, true) // 재시도
		return
	}

	msg.Ack(false)
	log.Printf("✅ 완료: %s (%.1fms, %d bytes)",
		req.TaskID, result.ProcessingMs, result.FileSizeBytes)
}
