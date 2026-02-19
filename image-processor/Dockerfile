# ============================================
# Go 이미지 처리 마이크로서비스 Dockerfile
# Multi-stage build: golang:1.26-alpine → alpine:3.21
# 최종 이미지 크기: ~15MB
# ============================================

# ── Stage 1: 빌드 ──
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /image-processor ./cmd/main.go

# ── Stage 2: 실행 ──
FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /image-processor .

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD wget -qO- http://localhost:8081/health || exit 1

EXPOSE 8081

ENTRYPOINT ["./image-processor"]
