// llm-retry-gateway — 本地 LLM 429 重试代理网关（单文件、Go 标准库 only）。
//
// Agent 把 OpenAI 兼容 base URL 指向 http://127.0.0.1:18888/https://upstream.host/v1，
// 网关剥离前缀后转发到真实上游；仅对 /chat/completions 的 HTTP 429 无限重试。
package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// 2. 常量
// ---------------------------------------------------------------------------

const max429BodyBytes = 64 << 10 // 65536，429 响应体固定上限，与 maxBodyBytes 解耦

var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Proxy-Connection",
}

const (
	errMsgHTTPNotAllowed = "http upstream not allowed; set allowHttp: true to enable"
	errMsgBodyTooLarge   = "request body too large"
)

// gatewayError 携带 HTTP 状态码，避免引入 errors 包（§1 的 import 白名单不含它）。
type gatewayError struct {
	code int
	msg  string
}

func (e *gatewayError) Error() string { return e.msg }

var errHTTPNotAllowed = &gatewayError{code: http.StatusBadRequest, msg: errMsgHTTPNotAllowed}

func errForbidden(host string) error {
	return &gatewayError{code: http.StatusForbidden, msg: "upstream host not allowed: " + host}
}

// ---------------------------------------------------------------------------
// 3. 类型
// ---------------------------------------------------------------------------

type RampConfig struct {
	DelaysMs    []int   `json:"delaysMs"`
	JitterRatio float64 `json:"jitterRatio"`
}

type Config struct {
	Listen                   string     `json:"listen"`
	AllowHttp                bool       `json:"allowHttp"`
	AllowedUpstreamHosts     []string   `json:"allowedUpstreamHosts"`
	MaxBodyBytes             int        `json:"maxBodyBytes"`
	LogLevel                 string     `json:"logLevel"`
	NormalRamp               RampConfig `json:"normalRamp"`
	AmbiguousRamp            RampConfig `json:"ambiguousRamp"`
	RetryAfterCapRatio       int        `json:"retryAfterCapRatio"`
	ResponseHeaderTimeoutSec int        `json:"responseHeaderTimeoutSec"`
	SmallRetryAfterFixed     bool       `json:"smallRetryAfterFixed"`
	Max5xxRetries            int        `json:"max5xxRetries"`
	QuotaExhaustedPatterns   []string   `json:"quotaExhaustedPatterns"`
	RateLimitPatterns        []string   `json:"rateLimitPatterns"`
}

type Classification int

const (
	ClassRateLimit Classification = iota
	ClassAmbiguousQuota
	ClassQuotaExhausted
	ClassUnclassified
)

func (c Classification) String() string {
	switch c {
	case ClassRateLimit:
		return "rate_limit"
	case ClassAmbiguousQuota:
		return "ambiguous_quota"
	case ClassQuotaExhausted:
		return "quota_exhausted"
	default:
		return "unclassified"
	}
}

type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

type Logger struct{ level LogLevel }

func (l *Logger) Debug(f string, a ...any) {
	if l.level <= LevelDebug {
		log.Printf("[DEBUG] "+f, a...)
	}
}
func (l *Logger) Info(f string, a ...any) {
	if l.level <= LevelInfo {
		log.Printf("[INFO]  "+f, a...)
	}
}
func (l *Logger) Warn(f string, a ...any) {
	if l.level <= LevelWarn {
		log.Printf("[WARN]  "+f, a...)
	}
}
func (l *Logger) Error(f string, a ...any) {
	if l.level <= LevelError {
		log.Printf("[ERROR] "+f, a...)
	}
}

func parseLogLevel(s string) LogLevel {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

// ---------------------------------------------------------------------------
// 4. HTTP client 构造（ResponseHeaderTimeout 由配置传入；main() 构造后经参数
//    传递给 handle / handleChat / handlePassthrough / forwardWithRetry，
//    无包级变量）
// ---------------------------------------------------------------------------

func newHTTPClient(respHeaderTimeout time.Duration) *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = respHeaderTimeout
	return &http.Client{
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// ---------------------------------------------------------------------------
// 5. main / 配置
// ---------------------------------------------------------------------------

func defaultConfig() *Config {
	return &Config{
		Listen:               "127.0.0.1:18888",
		AllowHttp:            false,
		AllowedUpstreamHosts: []string{},
		MaxBodyBytes:         262144,
		LogLevel:             "info",
		NormalRamp: RampConfig{
			DelaysMs:    []int{2000, 2000, 3000, 3000, 3000, 4500, 4500, 6750, 10000},
			JitterRatio: 0.25,
		},
		AmbiguousRamp: RampConfig{
			DelaysMs:    []int{4000, 6000, 9000, 13500, 20250, 30000},
			JitterRatio: 0.25,
		},
		RetryAfterCapRatio:       4,
		ResponseHeaderTimeoutSec: 120,
		SmallRetryAfterFixed:     false,
		Max5xxRetries:            0, // 默认不重试 5xx
		QuotaExhaustedPatterns: []string{
			"credit_balance_exhausted",
			"organization_usage_limit_exceeded",
			"organization_spend_limit_exceeded",
			"project_spend_limit_exceeded",
			"daily usage limit exceeded",
		},
		RateLimitPatterns: []string{
			"rate_limit_exceeded",
			"token_rate_limit_exceeded",
			"ip_rate_limit_exceeded",
			"concurrency_rate_limit_exceeded",
			"tpm exhausted",
			"rpm exhausted",
		},
	}
}

func loadConfig(path string) (*Config, error) {
	cfg := defaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read config %q: %w", path, err)
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("invalid config %q: %w", path, err)
	}

	if v := os.Getenv("LISTEN"); v != "" {
		cfg.Listen = v
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func validateConfig(cfg *Config) error {
	host, _, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: %w", cfg.Listen, err)
	}
	switch strings.ToLower(host) {
	case "127.0.0.1", "::1", "localhost":
	default:
		return fmt.Errorf("listen host %q is not loopback: only 127.0.0.1 / ::1 / localhost allowed", host)
	}
	if cfg.MaxBodyBytes < 1024 {
		return fmt.Errorf("maxBodyBytes must be >= 1024, got %d", cfg.MaxBodyBytes)
	}
	if cfg.RetryAfterCapRatio < 1 {
		return fmt.Errorf("retryAfterCapRatio must be >= 1, got %d", cfg.RetryAfterCapRatio)
	}
	if cfg.ResponseHeaderTimeoutSec < 1 {
		return fmt.Errorf("responseHeaderTimeoutSec must be >= 1, got %d", cfg.ResponseHeaderTimeoutSec)
	}
	if cfg.Max5xxRetries < 0 {
		return fmt.Errorf("max5xxRetries must be >= 0, got %d", cfg.Max5xxRetries)
	}
	for name, ramp := range map[string]RampConfig{
		"normalRamp":    cfg.NormalRamp,
		"ambiguousRamp": cfg.AmbiguousRamp,
	} {
		if len(ramp.DelaysMs) == 0 {
			return fmt.Errorf("%s.delaysMs must not be empty", name)
		}
		for _, v := range ramp.DelaysMs {
			if v <= 0 {
				return fmt.Errorf("%s.delaysMs values must be > 0, got %d", name, v)
			}
		}
		if ramp.JitterRatio < 0 || ramp.JitterRatio > 1 {
			return fmt.Errorf("%s.jitterRatio must be in [0,1], got %v", name, ramp.JitterRatio)
		}
	}
	return nil
}

func main() {
	configPath := flag.String("config", "config.json", "path to config file (relative to CWD)")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	logger := &Logger{level: parseLogLevel(cfg.LogLevel)}

	if len(cfg.AllowedUpstreamHosts) == 0 {
		logger.Warn("allowedUpstreamHosts is empty; gateway will forward to ANY host including internal network. Set it to a whitelist if this machine is shared.")
	}

	// 按配置构造上游 client，经参数传递给各 handler，不使用包级变量。
	respHeaderTimeout := time.Duration(cfg.ResponseHeaderTimeoutSec) * time.Second
	client := newHTTPClient(respHeaderTimeout)
	logger.Info("response_header_timeout=%s", respHeaderTimeout)

	srv := &http.Server{
		Addr: cfg.Listen,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handle(w, r, cfg, logger, client)
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening on %s", cfg.Listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("received signal %v, shutting down...", sig)
	case err := <-errCh:
		log.Printf("server error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 6. 请求处理
// ---------------------------------------------------------------------------

func handle(w http.ResponseWriter, r *http.Request, cfg *Config, logger *Logger, client *http.Client) {
	u, err := extractUpstreamURL(r, cfg)
	if err != nil {
		if ge, ok := err.(*gatewayError); ok {
			writeJSONError(w, ge.code, ge.msg)
		} else {
			writeJSONError(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	if strings.HasSuffix(u.Path, "/chat/completions") {
		handleChat(w, r, u, cfg, logger, client)
	} else {
		handlePassthrough(w, r, u, cfg, logger, client)
	}
}

func extractUpstreamURL(r *http.Request, cfg *Config) (*url.URL, error) {
	raw := r.RequestURI
	if raw == "" {
		raw = r.URL.EscapedPath()
		if r.URL.RawQuery != "" {
			raw += "?" + r.URL.RawQuery
		}
	}

	path, query := raw, ""
	if i := strings.Index(raw, "?"); i >= 0 {
		path, query = raw[:i], raw[i+1:]
	}

	var upstream string
	switch {
	case strings.HasPrefix(path, "/https://"):
		upstream = path[1:]
	case strings.HasPrefix(path, "/http://"):
		if !cfg.AllowHttp {
			return nil, errHTTPNotAllowed
		}
		upstream = path[1:]
	default:
		return nil, fmt.Errorf("missing upstream URL, expected path like /https://host/v1/...")
	}
	if query != "" {
		upstream += "?" + query
	}

	u, err := url.Parse(upstream)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	if u.Scheme == "http" && !cfg.AllowHttp {
		return nil, errHTTPNotAllowed
	}
	if u.Host == "" {
		return nil, fmt.Errorf("missing host in upstream URL")
	}

	if len(cfg.AllowedUpstreamHosts) > 0 {
		host := normalizeHost(u.Hostname())
		allowed := false
		for _, h := range cfg.AllowedUpstreamHosts {
			if host == normalizeHost(h) {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, errForbidden(u.Hostname())
		}
	}
	return u, nil
}

func normalizeHost(h string) string {
	h = strings.TrimSpace(strings.ToLower(h))
	if host, _, err := net.SplitHostPort(h); err == nil {
		h = host
	}
	h = strings.Trim(h, "[]")
	return strings.TrimSuffix(h, ".")
}

func handleChat(w http.ResponseWriter, r *http.Request, u *url.URL, cfg *Config, logger *Logger, client *http.Client) {
	ctx := r.Context()
	reqID := newRequestID()

	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, int64(cfg.MaxBodyBytes)+1))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(bodyBytes) > cfg.MaxBodyBytes {
		writeJSONError(w, http.StatusRequestEntityTooLarge, errMsgBodyTooLarge)
		return
	}

	resp, err := forwardWithRetry(ctx, u.String(), bodyBytes, r.Header, cfg, logger, reqID, client)
	if err != nil {
		if ctx.Err() != nil {
			return // 客户端已断，不写任何响应
		}
		writeJSONError(w, http.StatusBadGateway, "upstream request failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	streamResponse(ctx, w, resp, logger, reqID)
}

func handlePassthrough(w http.ResponseWriter, r *http.Request, u *url.URL, cfg *Config, logger *Logger, client *http.Client) {
	ctx := r.Context()
	reqID := newRequestID()

	req, err := http.NewRequestWithContext(ctx, r.Method, u.String(), r.Body)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "upstream request failed: "+err.Error())
		return
	}
	req.ContentLength = r.ContentLength

	copyHeader(req.Header, r.Header)
	req.Header.Del("Host")
	req.Header.Del("Content-Length")
	delHopByHop(req.Header)

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		writeJSONError(w, http.StatusBadGateway, "upstream request failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	streamResponse(ctx, w, resp, logger, reqID)
}

// ---------------------------------------------------------------------------
// 7. 重试循环
// ---------------------------------------------------------------------------

func forwardWithRetry(ctx context.Context, upstreamURL string, bodyBytes []byte,
	originalHeader http.Header, cfg *Config, logger *Logger, reqID string, client *http.Client) (*http.Response, error) {

	attempt := 0
	rampIndex := 0
	retry5xxCount := 0 // 502/503/504 的已用重试次数，独立于 429 的无限重试

	for {
		attempt++
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, err
		}

		copyHeader(req.Header, originalHeader)
		req.Header.Del("Host")
		req.Header.Del("Content-Length")
		delHopByHop(req.Header)
		// Authorization 已在 originalHeader 中，copyHeader 已透传。

		resp, err := client.Do(req)
		if ctx.Err() != nil {
			if resp != nil {
				resp.Body.Close()
			}
			return nil, ctx.Err()
		}
		if err != nil {
			// 仅 429 重试；网络错误不重放非幂等 POST，直接返回 error。
			logger.Warn("req=%s attempt=%d network_error=%v final=502", reqID, attempt, err)
			return nil, err
		}

		if resp.StatusCode != http.StatusTooManyRequests {
			// 502/503/504：按 max5xxRetries 有限重试；耗尽后拦截 HTML 包装为 OpenAI 兼容 JSON。
			if resp.StatusCode == http.StatusBadGateway ||
				resp.StatusCode == http.StatusServiceUnavailable ||
				resp.StatusCode == http.StatusGatewayTimeout {

				bodySnippet, _ := readLimited(resp.Body, max429BodyBytes)
				resp.Body.Close()

				if retry5xxCount < cfg.Max5xxRetries {
					retry5xxCount++
					ramp := &cfg.NormalRamp
					var capped time.Duration
					if ra := parseRetryAfter(resp.Header.Get("Retry-After")); ra > 0 {
						capDelay := time.Duration(maxInt(ramp.DelaysMs)) * time.Millisecond *
							time.Duration(cfg.RetryAfterCapRatio)
						capped = ra
						if capped > capDelay {
							capped = capDelay
						}
					}
					rampDelay := computeRampDelay(ramp, rampIndex)
					delay := max(capped, rampDelay) // Retry-After 是下限；max 为 Go 1.21+ 内建
					rampIndex++                     // 与 429 共享，保持退避曲线连续

					logger.Warn("req=%s attempt=%d status=%d final=5xx_retry capped_ms=%d ramp_ms=%d sleep_ms=%d before_attempt=%d",
						reqID, attempt, resp.StatusCode, capped.Milliseconds(), rampDelay.Milliseconds(),
						delay.Milliseconds(), attempt+1)
					if !sleepWithContext(ctx, delay) {
						return nil, ctx.Err()
					}
					continue
				}

				logger.Info("req=%s attempt=%d status=%d retries_5xx_used=%d final=5xx_intercept",
					reqID, attempt, resp.StatusCode, retry5xxCount)
				lower := strings.ToLower(string(bodySnippet))
				if strings.Contains(lower, "<html") || strings.Contains(lower, "<!doctype html") {
					// HTML 错误页会令 OpenAI 兼容 SDK 解析崩溃，包装为 JSON。
					errorJSON := fmt.Sprintf(`{"error": {"message": "Upstream returned %d with an HTML error page; intercepted and wrapped by llm-retry-gateway.", "type": "upstream_error"}}`, resp.StatusCode)
					resp = buildSyntheticResponse(resp, []byte(errorJSON))
					resp.Header.Set("Content-Type", "application/json; charset=utf-8")
					logger.Info("req=%s final=5xx_wrapped", reqID)
				} else {
					// JSON / 纯文本：原样透传。
					resp = buildSyntheticResponse(resp, bodySnippet)
				}
				return resp, nil
			}

			logger.Info("req=%s attempt=%d status=%d final=ok", reqID, attempt, resp.StatusCode)
			return resp, nil
		}

		bodySnippet, _ := readLimited(resp.Body, max429BodyBytes)
		resp.Body.Close()

		class := classify(bodySnippet, resp.Header, cfg)
		logger.Info("req=%s attempt=%d status=429 class=%v retry_after=%q upstream=%s",
			reqID, attempt, class, resp.Header.Get("Retry-After"), upstreamURL)

		if class == ClassQuotaExhausted {
			logger.Info("req=%s quota_exhausted final=passthrough", reqID)
			return buildSyntheticResponse(resp, bodySnippet), nil
		}

		ramp := rampFor(class, cfg)
		var capped time.Duration
		if ra := parseRetryAfter(resp.Header.Get("Retry-After")); ra > 0 {
			capDelay := time.Duration(maxInt(ramp.DelaysMs)) * time.Millisecond *
				time.Duration(cfg.RetryAfterCapRatio)
			capped = ra
			if capped > capDelay {
				capped = capDelay
			}
		}
		rampDelay := computeRampDelay(ramp, rampIndex)
		minDelayMs := minInt(ramp.DelaysMs)
		delay := decideDelay(capped, rampDelay, minDelayMs, ramp.JitterRatio, cfg.SmallRetryAfterFixed)
		rampIndex++ // 无论是否使用 Retry-After 都递增

		logger.Info("req=%s attempt=%d sleep_ms=%d small_ra_fixed=%v capped_ms=%d ramp_ms=%d min_ms=%d before_attempt=%d",
			reqID, attempt, delay.Milliseconds(), cfg.SmallRetryAfterFixed,
			capped.Milliseconds(), rampDelay.Milliseconds(), minDelayMs, attempt+1)
		if !sleepWithContext(ctx, delay) {
			return nil, ctx.Err()
		}
	}
}

func buildSyntheticResponse(orig *http.Response, bodyBytes []byte) *http.Response {
	h := orig.Header.Clone()
	h.Del("Transfer-Encoding")
	h.Set("Content-Length", strconv.Itoa(len(bodyBytes)))
	// 不删 Content-Encoding / Content-Type / Retry-After（端到端头，见 §10.3）。
	return &http.Response{
		Status:        orig.Status,
		StatusCode:    orig.StatusCode,
		Proto:         orig.Proto,
		ProtoMajor:    orig.ProtoMajor,
		ProtoMinor:    orig.ProtoMinor,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(bodyBytes)),
		ContentLength: int64(len(bodyBytes)),
		Request:       orig.Request,
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func rampFor(class Classification, cfg *Config) *RampConfig {
	if class == ClassAmbiguousQuota {
		return &cfg.AmbiguousRamp
	}
	return &cfg.NormalRamp
}

// ---------------------------------------------------------------------------
// 8. 分类器
// ---------------------------------------------------------------------------

func extractTextForMatching(bodyBytes []byte) string {
	var v map[string]any
	if err := json.Unmarshal(bodyBytes, &v); err != nil {
		return string(bodyBytes)
	}

	var parts []string
	addString := func(x any) {
		if s, ok := x.(string); ok {
			parts = append(parts, s)
		}
	}
	if e, ok := v["error"].(map[string]any); ok {
		addString(e["type"])
		addString(e["code"])
		addString(e["message"])
	} else {
		addString(v["error"]) // error 为字符串时也能参与匹配
	}
	addString(v["message"])
	addString(v["detail"])
	addString(v["code"])
	return strings.Join(parts, " ")
}

func classify(bodyBytes []byte, headers http.Header, cfg *Config) Classification {
	haystack := strings.ToLower(extractTextForMatching(bodyBytes))

	for _, p := range cfg.QuotaExhaustedPatterns {
		if strings.Contains(haystack, strings.ToLower(p)) {
			return ClassQuotaExhausted
		}
	}
	for _, p := range cfg.RateLimitPatterns {
		if strings.Contains(haystack, strings.ToLower(p)) {
			return ClassRateLimit
		}
	}
	if strings.Contains(haystack, "insufficient_quota") {
		return ClassAmbiguousQuota
	}
	if parseRetryAfter(headers.Get("Retry-After")) > 0 {
		return ClassRateLimit
	}
	return ClassUnclassified
}

func readLimited(body io.Reader, limit int) ([]byte, error) {
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 1024)
	for len(buf) < limit {
		n, err := body.Read(tmp[:min(len(tmp), limit-len(buf))])
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return buf, err
		}
	}
	return buf, nil
}

// ---------------------------------------------------------------------------
// 9. 退避计算
// ---------------------------------------------------------------------------

// decideDelay 计算重试前的等待时间。
// 当 fixed 为 true 且上游 Retry-After 解析结果 <= 所选 ramp 的 min(delaysMs) 时，
// 延迟固定为 min(delaysMs)×抖动，不再与 rampDelay 取 max（即跳过爬坡序列）；
// 其余情况保持 max(capped, rampDelay)。抽取为纯函数以便单测覆盖两条分支。
func decideDelay(capped, rampDelay time.Duration, minDelayMs int, jitterRatio float64, fixed bool) time.Duration {
	if fixed && capped > 0 && capped <= time.Duration(minDelayMs)*time.Millisecond {
		return jitterDuration(minDelayMs, jitterRatio)
	}
	return max(capped, rampDelay)
}

func computeRampDelay(ramp *RampConfig, index int) time.Duration {
	v := ramp.DelaysMs[index%len(ramp.DelaysMs)]
	return jitterDuration(v, ramp.JitterRatio)
}

// jitterDuration 返回 v×(1±ratio) 的随机时长，保底非负。
// computeRampDelay 与 decideDelay 共用同一抖动公式，保证语义一致。
func jitterDuration(v int, ratio float64) time.Duration {
	jitter := float64(v) * ratio
	actual := float64(v) - jitter + mrand.Float64()*2*jitter
	if actual < 0 {
		actual = 0
	}
	return time.Duration(actual) * time.Millisecond
}

func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if sec, err := strconv.Atoi(v); err == nil && sec >= 0 {
		return time.Duration(sec) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d
	}
	return 0
}

// ---------------------------------------------------------------------------
// 10. 流式透传 / Header / 错误响应
// ---------------------------------------------------------------------------

func streamResponse(ctx context.Context, w http.ResponseWriter, resp *http.Response, logger *Logger, reqID string) {
	delHopByHop(resp.Header)
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				// 客户端断开时，Write 通常会立刻报错
				logger.Debug("req=%s stream write error (client gone?): %v", reqID, werr)
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			if err != io.EOF {
				if ctx.Err() != nil {
					// 客户端断开导致请求 context 取消：正常高频现象，降级为 Debug
					logger.Debug("req=%s stream closed by client", reqID)
				} else {
					// context 没取消但 Read 报错，说明是上游服务端异常中断，保留 Warn
					logger.Warn("req=%s stream copy error: %v", reqID, err)
				}
			}
			return
		}
	}
}

func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

// delHopByHop 先展开 Connection / Proxy-Connection 的 token，再删固定 9 项。
// 必须用 Values 而非 Get：Get 只返回第一行，多个 Connection 行会漏删 token。
func delHopByHop(h http.Header) {
	for _, key := range []string{"Connection", "Proxy-Connection"} {
		for _, v := range h.Values(key) {
			for _, token := range strings.Split(v, ",") {
				if t := strings.TrimSpace(token); t != "" {
					h.Del(t)
				}
			}
		}
	}
	for _, k := range hopByHopHeaders {
		h.Del(k)
	}
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ---------------------------------------------------------------------------
// 11. 工具
// ---------------------------------------------------------------------------

func newRequestID() string {
	b := make([]byte, 4)
	crand.Read(b)
	return fmt.Sprintf("%x", b)
}

func minInt(xs []int) int {
	m := xs[0]
	for _, v := range xs[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func maxInt(xs []int) int {
	m := xs[0]
	for _, v := range xs[1:] {
		if v > m {
			m = v
		}
	}
	return m
}
