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
	Listen                 string     `json:"listen"`
	AllowHttp              bool       `json:"allowHttp"`
	AllowedUpstreamHosts   []string   `json:"allowedUpstreamHosts"`
	MaxBodyBytes           int        `json:"maxBodyBytes"`
	LogLevel               string     `json:"logLevel"`
	NormalRamp             RampConfig `json:"normalRamp"`
	AmbiguousRamp          RampConfig `json:"ambiguousRamp"`
	RetryAfterCapRatio     int        `json:"retryAfterCapRatio"`
	QuotaExhaustedPatterns []string   `json:"quotaExhaustedPatterns"`
	RateLimitPatterns      []string   `json:"rateLimitPatterns"`
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
// 4. 包级共享 client（forwardWithRetry 与 handlePassthrough 共用，避免每请求
//    Clone() 产生新连接池）
// ---------------------------------------------------------------------------

func newHTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = 60 * time.Second
	return &http.Client{
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

var upstreamClient = newHTTPClient()

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
		RetryAfterCapRatio: 4,
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

	srv := &http.Server{
		Addr: cfg.Listen,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handle(w, r, cfg, logger)
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

func handle(w http.ResponseWriter, r *http.Request, cfg *Config, logger *Logger) {
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
		handleChat(w, r, u, cfg, logger)
	} else {
		handlePassthrough(w, r, u, cfg, logger)
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

func handleChat(w http.ResponseWriter, r *http.Request, u *url.URL, cfg *Config, logger *Logger) {
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

	resp, err := forwardWithRetry(ctx, u.String(), bodyBytes, r.Header, cfg, logger, reqID)
	if err != nil {
		if ctx.Err() != nil {
			return // 客户端已断，不写任何响应
		}
		writeJSONError(w, http.StatusBadGateway, "upstream request failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	streamResponse(w, resp, logger, reqID)
}

func handlePassthrough(w http.ResponseWriter, r *http.Request, u *url.URL, cfg *Config, logger *Logger) {
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

	resp, err := upstreamClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		writeJSONError(w, http.StatusBadGateway, "upstream request failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	streamResponse(w, resp, logger, reqID)
}

// ---------------------------------------------------------------------------
// 7. 重试循环
// ---------------------------------------------------------------------------

func forwardWithRetry(ctx context.Context, upstreamURL string, bodyBytes []byte,
	originalHeader http.Header, cfg *Config, logger *Logger, reqID string) (*http.Response, error) {

	attempt := 0
	rampIndex := 0

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

		resp, err := upstreamClient.Do(req)
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
		delay := max(capped, rampDelay) // Retry-After 是下限；max 为 Go 1.21+ 内建
		rampIndex++                     // 无论是否使用 Retry-After 都递增

		logger.Info("req=%s attempt=%d sleep_ms=%d before_attempt=%d",
			reqID, attempt, delay.Milliseconds(), attempt+1)
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

func computeRampDelay(ramp *RampConfig, index int) time.Duration {
	v := ramp.DelaysMs[index%len(ramp.DelaysMs)]
	jitter := float64(v) * ramp.JitterRatio
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

func streamResponse(w http.ResponseWriter, resp *http.Response, logger *Logger, reqID string) {
	delHopByHop(resp.Header)
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			if err != io.EOF {
				logger.Warn("req=%s stream copy error: %v", reqID, err)
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

func maxInt(xs []int) int {
	m := xs[0]
	for _, v := range xs[1:] {
		if v > m {
			m = v
		}
	}
	return m
}
