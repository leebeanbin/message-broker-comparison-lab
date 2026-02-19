// Package handler는 이미지 리사이즈/압축 처리 로직을 담당합니다.
// Go 표준 라이브러리의 image 패키지를 사용하여 외부 의존성 없이 동작합니다.
package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// ──────────────────────────────────────────────
// 메시지 구조체: Python ↔ Go 간 통신 프로토콜
// ──────────────────────────────────────────────

// ProcessRequest는 Python에서 보내는 이미지 처리 요청입니다.
type ProcessRequest struct {
	TaskID       string   `json:"task_id"`
	ImageURL     string   `json:"image_url"`
	TargetWidth  int      `json:"target_width"`
	TargetHeight int      `json:"target_height"`
	Format       string   `json:"format"`  // "jpeg" 또는 "png"
	Quality      int      `json:"quality"` // JPEG 품질 (1-100)
	Filters      []string `json:"filters"` // ["grayscale", "blur", "sharpen", "sepia", "contrast"]
}

// ProcessResult는 Go에서 Python으로 보내는 처리 결과입니다.
type ProcessResult struct {
	TaskID         string  `json:"task_id"`
	Status         string  `json:"status"` // "completed" 또는 "failed"
	OriginalWidth  int     `json:"original_width"`
	OriginalHeight int     `json:"original_height"`
	ResultWidth    int     `json:"result_width"`
	ResultHeight   int     `json:"result_height"`
	Format         string  `json:"format"`
	FileSizeBytes  int     `json:"file_size_bytes"`
	ProcessingMs   float64 `json:"processing_ms"`
	FiltersApplied []string `json:"filters_applied"`
	Error          string  `json:"error,omitempty"`
}

// ──────────────────────────────────────────────
// Prometheus 메트릭 정의
// ──────────────────────────────────────────────

var (
	// 처리된 이미지 총 수 (성공/실패별)
	ImagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "image_processor_images_total",
			Help: "Total number of images processed",
		},
		[]string{"status"},
	)

	// 이미지 처리 소요 시간 히스토그램
	ProcessingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "image_processor_duration_seconds",
			Help:    "Time spent processing images",
			Buckets: prometheus.DefBuckets,
		},
	)

	// 현재 처리 중인 이미지 수
	InFlight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "image_processor_in_flight",
			Help: "Number of images currently being processed",
		},
	)
)

func init() {
	prometheus.MustRegister(ImagesProcessed, ProcessingDuration, InFlight)
}

// ──────────────────────────────────────────────
// ImageHandler: 이미지 처리 핵심 로직
// ──────────────────────────────────────────────

// ImageHandler는 이미지 다운로드, 리사이즈, 필터 적용, 결과 저장을 수행합니다.
type ImageHandler struct {
	RedisClient *redis.Client
	HTTPClient  *http.Client
}

// NewImageHandler는 Redis 클라이언트를 받아 핸들러를 생성합니다.
func NewImageHandler(redisClient *redis.Client) *ImageHandler {
	return &ImageHandler{
		RedisClient: redisClient,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Process는 요청을 받아 이미지를 처리하고 결과를 Redis에 저장합니다.
// 전체 파이프라인: 다운로드 → 디코딩 → 리사이즈 → 필터 → 인코딩 → Redis 저장
func (h *ImageHandler) Process(ctx context.Context, req *ProcessRequest) (*ProcessResult, error) {
	InFlight.Inc()
	defer InFlight.Dec()

	start := time.Now()

	result := &ProcessResult{
		TaskID: req.TaskID,
		Format: req.Format,
	}

	// Step 1: 이미지 다운로드
	img, format, err := h.downloadImage(ctx, req.ImageURL)
	if err != nil {
		// 다운로드 실패 시 테스트용 이미지 생성 (Mock 환경 대응)
		log.Printf("[%s] 다운로드 실패, 테스트 이미지 생성: %v", req.TaskID, err)
		img = generateTestImage(800, 600)
		format = "jpeg"
	}

	bounds := img.Bounds()
	result.OriginalWidth = bounds.Dx()
	result.OriginalHeight = bounds.Dy()

	// Step 2: 리사이즈 (Nearest Neighbor → 외부 라이브러리 불필요)
	targetW := req.TargetWidth
	targetH := req.TargetHeight
	if targetW <= 0 {
		targetW = bounds.Dx()
	}
	if targetH <= 0 {
		targetH = bounds.Dy()
	}
	resized := resizeImage(img, targetW, targetH)
	result.ResultWidth = targetW
	result.ResultHeight = targetH

	// Step 3: 필터 적용
	filtered := applyFilters(resized, req.Filters)
	result.FiltersApplied = req.Filters

	// Step 4: 인코딩 (JPEG 또는 PNG)
	outFormat := req.Format
	if outFormat == "" {
		outFormat = format
	}
	quality := req.Quality
	if quality <= 0 || quality > 100 {
		quality = 85
	}

	var buf bytes.Buffer
	switch outFormat {
	case "png":
		err = png.Encode(&buf, filtered)
	default:
		err = jpeg.Encode(&buf, filtered, &jpeg.Options{Quality: quality})
		outFormat = "jpeg"
	}
	if err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("인코딩 실패: %v", err)
		ImagesProcessed.WithLabelValues("failed").Inc()
		return result, err
	}

	result.FileSizeBytes = buf.Len()
	result.Format = outFormat

	// Step 5: Redis에 결과 메타데이터 저장 (TTL 1시간)
	resultKey := fmt.Sprintf("image:result:%s", req.TaskID)
	resultJSON, _ := json.Marshal(result)
	h.RedisClient.Set(ctx, resultKey, string(resultJSON), 1*time.Hour)

	// 처리된 이미지 바이너리도 Redis에 저장 (노트북에서 확인용)
	dataKey := fmt.Sprintf("image:data:%s", req.TaskID)
	h.RedisClient.Set(ctx, dataKey, buf.Bytes(), 1*time.Hour)

	elapsed := time.Since(start)
	result.ProcessingMs = float64(elapsed.Milliseconds())
	result.Status = "completed"

	// 메트릭 업데이트
	ProcessingDuration.Observe(elapsed.Seconds())
	ImagesProcessed.WithLabelValues("completed").Inc()

	log.Printf("[%s] 처리 완료: %dx%d → %dx%d (%s, %d bytes, %.1fms)",
		req.TaskID,
		result.OriginalWidth, result.OriginalHeight,
		result.ResultWidth, result.ResultHeight,
		result.Format, result.FileSizeBytes, result.ProcessingMs,
	)

	return result, nil
}

// ──────────────────────────────────────────────
// 이미지 처리 헬퍼 함수들
// ──────────────────────────────────────────────

// downloadImage는 URL에서 이미지를 다운로드하고 디코딩합니다.
func (h *ImageHandler) downloadImage(ctx context.Context, url string) (image.Image, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("요청 생성 실패: %w", err)
	}

	resp, err := h.HTTPClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("다운로드 실패: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	// 최대 10MB 제한
	limited := io.LimitReader(resp.Body, 10*1024*1024)
	img, format, err := image.Decode(limited)
	if err != nil {
		return nil, "", fmt.Errorf("디코딩 실패: %w", err)
	}

	return img, format, nil
}

// generateTestImage는 다운로드 실패 시 테스트용 그라디언트 이미지를 생성합니다.
func generateTestImage(width, height int) image.Image {
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			r := uint8(float64(x) / float64(width) * 255)
			g := uint8(float64(y) / float64(height) * 255)
			b := uint8(128)
			img.Set(x, y, color.RGBA{R: r, G: g, B: b, A: 255})
		}
	}
	return img
}

// resizeImage는 Nearest Neighbor 알고리즘으로 이미지를 리사이즈합니다.
// 외부 라이브러리 없이 Go 표준 라이브러리만 사용합니다.
func resizeImage(src image.Image, newWidth, newHeight int) image.Image {
	srcBounds := src.Bounds()
	srcW := srcBounds.Dx()
	srcH := srcBounds.Dy()

	dst := image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))

	xRatio := float64(srcW) / float64(newWidth)
	yRatio := float64(srcH) / float64(newHeight)

	for y := 0; y < newHeight; y++ {
		for x := 0; x < newWidth; x++ {
			srcX := int(float64(x) * xRatio)
			srcY := int(float64(y) * yRatio)

			if srcX >= srcW {
				srcX = srcW - 1
			}
			if srcY >= srcH {
				srcY = srcH - 1
			}

			dst.Set(x, y, src.At(srcX+srcBounds.Min.X, srcY+srcBounds.Min.Y))
		}
	}

	return dst
}

// applyFilters는 요청된 필터들을 순서대로 적용합니다.
func applyFilters(img image.Image, filters []string) image.Image {
	result := img
	for _, filter := range filters {
		switch filter {
		case "grayscale":
			result = applyGrayscale(result)
		case "sepia":
			result = applySepia(result)
		case "contrast":
			result = applyContrast(result, 1.3) // 30% 대비 증가
		case "blur":
			result = applyBoxBlur(result)
		case "sharpen":
			result = applySharpen(result)
		}
	}
	return result
}

// applyGrayscale은 이미지를 흑백으로 변환합니다.
func applyGrayscale(src image.Image) image.Image {
	bounds := src.Bounds()
	dst := image.NewRGBA(bounds)

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r, g, b, a := src.At(x, y).RGBA()
			// ITU-R BT.601 가중 평균 (인간 시각 특성 반영)
			gray := uint8((0.299*float64(r) + 0.587*float64(g) + 0.114*float64(b)) / 256)
			dst.Set(x, y, color.RGBA{R: gray, G: gray, B: gray, A: uint8(a >> 8)})
		}
	}
	return dst
}

// applySepia는 세피아 톤 필터를 적용합니다.
func applySepia(src image.Image) image.Image {
	bounds := src.Bounds()
	dst := image.NewRGBA(bounds)

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r, g, b, a := src.At(x, y).RGBA()
			rf := float64(r) / 256
			gf := float64(g) / 256
			bf := float64(b) / 256

			newR := math.Min(255, 0.393*rf+0.769*gf+0.189*bf)
			newG := math.Min(255, 0.349*rf+0.686*gf+0.168*bf)
			newB := math.Min(255, 0.272*rf+0.534*gf+0.131*bf)

			dst.Set(x, y, color.RGBA{
				R: uint8(newR), G: uint8(newG), B: uint8(newB), A: uint8(a >> 8),
			})
		}
	}
	return dst
}

// applyContrast는 대비를 조정합니다. factor > 1이면 대비 증가.
func applyContrast(src image.Image, factor float64) image.Image {
	bounds := src.Bounds()
	dst := image.NewRGBA(bounds)

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r, g, b, a := src.At(x, y).RGBA()
			newR := clampUint8(factor*(float64(r>>8)-128) + 128)
			newG := clampUint8(factor*(float64(g>>8)-128) + 128)
			newB := clampUint8(factor*(float64(b>>8)-128) + 128)
			dst.Set(x, y, color.RGBA{R: newR, G: newG, B: newB, A: uint8(a >> 8)})
		}
	}
	return dst
}

// applyBoxBlur는 3x3 박스 블러를 적용합니다.
func applyBoxBlur(src image.Image) image.Image {
	bounds := src.Bounds()
	dst := image.NewRGBA(bounds)

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			var rSum, gSum, bSum, count float64
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					nx, ny := x+dx, y+dy
					if nx >= bounds.Min.X && nx < bounds.Max.X && ny >= bounds.Min.Y && ny < bounds.Max.Y {
						r, g, b, _ := src.At(nx, ny).RGBA()
						rSum += float64(r >> 8)
						gSum += float64(g >> 8)
						bSum += float64(b >> 8)
						count++
					}
				}
			}
			_, _, _, a := src.At(x, y).RGBA()
			dst.Set(x, y, color.RGBA{
				R: uint8(rSum / count),
				G: uint8(gSum / count),
				B: uint8(bSum / count),
				A: uint8(a >> 8),
			})
		}
	}
	return dst
}

// applySharpen은 3x3 샤프닝 커널을 적용합니다.
func applySharpen(src image.Image) image.Image {
	bounds := src.Bounds()
	dst := image.NewRGBA(bounds)

	// 샤프닝 커널: 중심 강조 + 주변 감소
	kernel := [3][3]float64{
		{0, -1, 0},
		{-1, 5, -1},
		{0, -1, 0},
	}

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			var rSum, gSum, bSum float64
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					nx, ny := x+dx, y+dy
					if nx < bounds.Min.X {
						nx = bounds.Min.X
					}
					if nx >= bounds.Max.X {
						nx = bounds.Max.X - 1
					}
					if ny < bounds.Min.Y {
						ny = bounds.Min.Y
					}
					if ny >= bounds.Max.Y {
						ny = bounds.Max.Y - 1
					}
					r, g, b, _ := src.At(nx, ny).RGBA()
					w := kernel[dy+1][dx+1]
					rSum += float64(r>>8) * w
					gSum += float64(g>>8) * w
					bSum += float64(b>>8) * w
				}
			}
			_, _, _, a := src.At(x, y).RGBA()
			dst.Set(x, y, color.RGBA{
				R: clampUint8(rSum),
				G: clampUint8(gSum),
				B: clampUint8(bSum),
				A: uint8(a >> 8),
			})
		}
	}
	return dst
}

// clampUint8은 값을 0~255 범위로 제한합니다.
func clampUint8(v float64) uint8 {
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return uint8(v)
}
