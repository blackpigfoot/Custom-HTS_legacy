package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Logger — slog 래퍼.
//
// 왜 래핑하는가:
//   - 파일 + stdout 동시 출력 (slog 기본은 단일 핸들러)
//   - 컴포넌트 식별자(broker, account 등)를 With()로 미리 바인딩
//   - 파일 핸들러 생명주기(Close) 관리
//   - 앱 전역 로거와 컴포넌트 로거를 동일 인터페이스로 사용
//
// 사용:
//
//	// 앱 시작 시 전역 로거 초기화
//	log, err := logger.New(cfg.Debug)
//	defer log.Close()
//
//	// 컴포넌트별 로거 파생 (원본 로거 유지)
//	brokerLog := log.With("broker", "kis", "account", "kis-main")
//	brokerLog.Info("connected")
type Logger struct {
	slog    *slog.Logger
	file    *os.File // nil이면 파일 출력 없음. Close()에서 정리.
}

// New — Logger 생성.
//
// DebugConfig를 받아 레벨, verbose, 파일 출력을 설정.
// 파일 출력이 설정되어 있으면 stdout + 파일 동시 출력.
func New(cfg DebugConfig) (*Logger, error) {
	level := parseLevel(cfg.LogLevel)

	opts := &slog.HandlerOptions{Level: level}

	var writers []io.Writer
	writers = append(writers, os.Stdout)

	var f *os.File
	if cfg.LogDir != "" {
		var err error
		f, err = openLogFile(cfg.LogDir)
		if err != nil {
			return nil, fmt.Errorf("opening log file: %w", err)
		}
		writers = append(writers, f)
	}

	w := io.MultiWriter(writers...)

	// JSON 핸들러: 파일 저장 및 구조화 로그에 적합.
	// 터미널 가독성이 필요하면 slog.NewTextHandler로 교체 가능.
	handler := slog.NewJSONHandler(w, opts)

	return &Logger{
		slog: slog.New(handler),
		file: f,
	}, nil
}

// Default — 파일 없이 stdout만 출력하는 기본 로거.
// 설정 로드 전 초기화 단계에서 사용.
func Default() *Logger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return &Logger{slog: slog.New(handler)}
}

// With — 컴포넌트 식별자를 바인딩한 새 Logger 반환.
//
// 원본 Logger는 변경되지 않음.
// 반환된 Logger의 모든 로그에 지정한 키-값이 자동으로 포함됨.
//
// 사용:
//
//	brokerLog := appLog.With("broker", "kis", "account", "kis-main")
//	brokerLog.Info("subscribed", "code", "005930")
//	// → {"broker":"kis","account":"kis-main","msg":"subscribed","code":"005930"}
func (l *Logger) With(args ...any) *Logger {
	return &Logger{
		slog: l.slog.With(args...),
		file: l.file, // 파일 핸들은 공유 (Close는 원본에서만)
	}
}

// WithContext — context를 포함한 로그 출력용 Logger 반환.
// trace ID 등 context 값을 로그에 포함할 때 사용. (Stage 2 이후 확장)
func (l *Logger) WithContext(ctx context.Context) *slog.Logger {
	return l.slog.With("ctx", ctx)
}

// Debug — 디버그 로그. Verbose=true or LogLevel=debug 시 출력.
func (l *Logger) Debug(msg string, args ...any) {
	l.slog.Debug(msg, args...)
}

// Info — 일반 정보 로그.
func (l *Logger) Info(msg string, args ...any) {
	l.slog.Info(msg, args...)
}

// Warn — 경고 로그. 처리는 됐지만 주의가 필요한 상황.
func (l *Logger) Warn(msg string, args ...any) {
	l.slog.Warn(msg, args...)
}

// Error — 에러 로그. err 키로 error 값을 전달하는 것이 관용적.
//
// 사용:
//
//	log.Error("trade parse failed", "err", err, "raw", rawData)
func (l *Logger) Error(msg string, args ...any) {
	l.slog.Error(msg, args...)
}

// Close — 파일 핸들러 정리. 앱 종료 시 defer로 호출.
// 파일 없이 생성된 Logger에서는 no-op.
func (l *Logger) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// DebugConfig — 로깅/디버그 설정.
//
// ServerConfig가 아닌 독립 구조체로 분리한 이유:
//   - 서버 설정(포트/호스트)과 진단 설정은 관심사가 다름
//   - 로깅 설정은 앱 시작 시 한 번 읽어 불변으로 사용
//   - 향후 CLI 플래그로도 오버라이드 가능한 구조
type DebugConfig struct {
	// Verbose — WS 수신 메시지 원문, REST 요청/응답 상세 출력.
	// true이면 LogLevel이 자동으로 debug로 낮아짐.
	// 운영 환경에서는 false 유지.
	Verbose bool `json:"verbose"`

	// LogLevel — 출력할 최소 로그 레벨.
	// "debug", "info", "warn", "error". 기본 "info".
	// Verbose=true이면 이 값보다 낮아도 debug 레벨로 강제.
	LogLevel string `json:"log_level"`

	// LogDir — 로그 파일을 저장할 디렉토리.
	// 빈 문자열이면 파일 저장 없이 stdout만 출력.
	// 파일명은 자동 생성: trading-YYYYMMDD.log
	LogDir string `json:"log_dir"`
}

// parseLevel — 문자열 로그 레벨을 slog.Level로 변환.
func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// openLogFile — 날짜별 로그 파일 열기 (없으면 생성).
//
// 파일명: trading-YYYYMMDD.log
// 같은 날 재시작해도 같은 파일에 이어쓰기.
func openLogFile(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("creating log dir: %w", err)
	}
	name := fmt.Sprintf("trading-%s.log", time.Now().Format("20060102"))
	path := filepath.Join(dir, name)
	return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
}
