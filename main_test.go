package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

func testLogger() *Logger { return &Logger{level: LevelError} }

// tinyRamp 让端到端测试不必真的等几秒。
func testConfig() *Config {
	cfg := defaultConfig()
	cfg.AllowHttp = true
	cfg.MaxBodyBytes = 1 << 20
	cfg.NormalRamp = RampConfig{DelaysMs: []int{1, 1, 1}, JitterRatio: 0}
	cfg.AmbiguousRamp = RampConfig{DelaysMs: []int{2, 2, 2}, JitterRatio: 0}
	return cfg
}

func startGateway(t *testing.T, cfg *Config) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handle(w, r, cfg, testLogger())
	}))
}

// gatewayTarget 拼出带 // 前缀的目标 URL，同时验证 Go 客户端不会归一化 //。
func gatewayTarget(gw, upstream *httptest.Server, path string) string {
	return gw.URL + "/http://" + strings.TrimPrefix(upstream.URL, "http://") + path
}

// ---------------------------------------------------------------------------
// 4. extractUpstreamURL / normalizeHost
// ---------------------------------------------------------------------------

func TestNormalizeHost(t *testing.T) {
	cases := map[string]string{
		"api.example.com:443": "api.example.com",
		"api.example.com":     "api.example.com",
		"[::1]:443":           "::1",
		"[::1]":               "::1",
		"::1":                 "::1",
		"127.0.0.1:18888":     "127.0.0.1",
		"localhost":           "localhost",
		"localhost:18888":     "localhost",
		"API.Example.COM.":    "api.example.com",
		" api.example.com ":   "api.example.com",
	}
	for in, want := range cases {
		if got := normalizeHost(in); got != want {
			t.Errorf("normalizeHost(%q) = %q, want %q", in, got, want)
		}
	}
}

func reqWithURI(target string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{"model":"x"}`))
	// 显式保留原始 RequestURI（含 //）
	r.RequestURI = target
	return r
}

func TestExtractUpstreamURL(t *testing.T) {
	cfg := testConfig()

	u, err := extractUpstreamURL(reqWithURI("/https://api.openai.com/v1/chat/completions?foo=1"), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.Scheme != "https" || u.Hostname() != "api.openai.com" || u.Path != "/v1/chat/completions" {
		t.Fatalf("bad parse: scheme=%q host=%q path=%q", u.Scheme, u.Hostname(), u.Path)
	}
	if u.RawQuery != "foo=1" {
		t.Fatalf("query lost: %q", u.RawQuery)
	}

	// http:// 上游：allowHttp=false 时拒绝，错误消息含配置项名
	cfgNoHTTP := testConfig()
	cfgNoHTTP.AllowHttp = false
	_, err = extractUpstreamURL(reqWithURI("/http://host/v1/chat/completions"), cfgNoHTTP)
	if err != errHTTPNotAllowed {
		t.Fatalf("want errHTTPNotAllowed, got %v", err)
	}
	if !strings.Contains(err.Error(), "allowHttp: true") {
		t.Fatalf("http-upstream error must name the config option: %q", err.Error())
	}

	// allowHttp=true 时放行
	if _, err := extractUpstreamURL(reqWithURI("/http://host/v1/chat/completions"), cfg); err != nil {
		t.Fatalf("allowHttp=true should permit http upstream: %v", err)
	}

	// 缺前缀 / 非法 scheme
	for _, target := range []string{"/v1/models", "/ftp://example.com/"} {
		if _, err := extractUpstreamURL(reqWithURI(target), cfg); err == nil {
			t.Errorf("expected error for %q", target)
		}
	}
}

func TestExtractUpstreamURL_HTTP2Fallback(t *testing.T) {
	// RequestURI 为空时走 EscapedPath 回退（防御性分支）
	r := httptest.NewRequest(http.MethodPost, "/https://api.openai.com/v1/chat/completions", nil)
	r.RequestURI = ""
	u, err := extractUpstreamURL(r, testConfig())
	if err != nil {
		t.Fatalf("fallback failed: %v", err)
	}
	if u.Hostname() != "api.openai.com" {
		t.Fatalf("bad host: %q", u.Hostname())
	}
}

func TestExtractUpstreamURL_WhitelistNormalize(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedUpstreamHosts = []string{"API.Example.com:443", "[::1]"}

	for _, target := range []string{
		"/https://api.example.com/v1/models",
		"/https://api.example.com:8443/v1/models",
		"/https://[::1]:443/v1/models",
	} {
		if _, err := extractUpstreamURL(reqWithURI(target), cfg); err != nil {
			t.Errorf("whitelist should allow %q: %v", target, err)
		}
	}
	if _, err := extractUpstreamURL(reqWithURI("/https://evil.example.com/v1/models"), cfg); err == nil {
		t.Error("host outside whitelist should be rejected")
	} else if ge, ok := err.(*gatewayError); !ok || ge.code != http.StatusForbidden {
		t.Errorf("want 403 gatewayError, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 5. 分类器
// ---------------------------------------------------------------------------

func TestClassify(t *testing.T) {
	cfg := testConfig()
	cases := []struct {
		name       string
		body       string
		retryAfter string
		want       Classification
	}{
		{"quota pattern wins over Retry-After", `{"error":{"type":"insufficient_quota","code":"credit_balance_exhausted"}}`, "30", ClassQuotaExhausted},
		{"quota pattern case-insensitive", `{"error":{"message":"DAILY USAGE LIMIT EXCEEDED"}}`, "", ClassQuotaExhausted},
		{"rate limit pattern", `{"error":{"type":"rate_limit_exceeded"}}`, "", ClassRateLimit},
		{"insufficient_quota beats Retry-After (E)", `{"error":{"type":"insufficient_quota","message":"quota issue"}}`, "1", ClassAmbiguousQuota},
		{"Retry-After as timing signal", `{"error":{"message":"slow down"}}`, "5", ClassRateLimit},
		{"bare 429 must not match", `{"message":"429"}`, "", ClassUnclassified},
		{"unclassified", `{"error":{"message":"unknown"}}`, "", ClassUnclassified},
		{"JSON parse failure falls back to raw body", `credit_balance_exhausted`, "", ClassQuotaExhausted},
		{"error as plain string", `{"error":"rate_limit_exceeded"}`, "", ClassRateLimit},
		{"Retry-After: 0 is not a valid signal", `{"error":{"message":"x"}}`, "0", ClassUnclassified},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := http.Header{}
			if c.retryAfter != "" {
				h.Set("Retry-After", c.retryAfter)
			}
			if got := classify([]byte(c.body), h, cfg); got != c.want {
				t.Errorf("classify = %v, want %v", got, c.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 7. 退避 / Retry-After
// ---------------------------------------------------------------------------

func TestComputeRampDelay(t *testing.T) {
	ramp := &RampConfig{DelaysMs: []int{2000, 3000}, JitterRatio: 0}
	for i := 0; i < 6; i++ {
		want := time.Duration(ramp.DelaysMs[i%2]) * time.Millisecond
		if got := computeRampDelay(ramp, i); got != want {
			t.Errorf("index %d: got %v, want %v", i, got, want)
		}
	}

	jRamp := &RampConfig{DelaysMs: []int{1000}, JitterRatio: 0.25}
	for i := 0; i < 200; i++ {
		d := computeRampDelay(jRamp, i)
		if d < 750*time.Millisecond || d > 1250*time.Millisecond {
			t.Fatalf("jitter out of range: %v", d)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	if got := parseRetryAfter("7"); got != 7*time.Second {
		t.Errorf("seconds: got %v", got)
	}
	if got := parseRetryAfter(""); got != 0 {
		t.Errorf("empty: got %v", got)
	}
	if got := parseRetryAfter("not-a-date"); got != 0 {
		t.Errorf("garbage: got %v", got)
	}
	future := time.Now().Add(90 * time.Minute).UTC().Format(http.TimeFormat)
	if got := parseRetryAfter(future); got <= 0 || got > 91*time.Minute {
		t.Errorf("http-date: got %v", got)
	}
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	if got := parseRetryAfter(past); got != 0 {
		t.Errorf("past date should clamp to 0, got %v", got)
	}
}

func TestSleepWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepWithContext(ctx, time.Hour) {
		t.Error("canceled context should return false immediately")
	}
	if !sleepWithContext(context.Background(), 0) {
		t.Error("zero duration should return true")
	}
}

// ---------------------------------------------------------------------------
// 10. Header / 错误响应 / 工具
// ---------------------------------------------------------------------------

func TestDelHopByHop(t *testing.T) {
	h := http.Header{}
	h.Add("Connection", "X-Custom-A, keep-alive")
	h.Add("Connection", "X-Custom-B")
	h.Add("Proxy-Connection", "X-Proxy-Tok")
	h.Set("Trailer", "X-Foo")
	h.Set("Transfer-Encoding", "chunked")
	h.Set("Authorization", "Bearer sk-test")
	h.Set("Content-Encoding", "gzip")
	h.Set("Content-Type", "application/json")

	delHopByHop(h)

	for _, key := range []string{"Connection", "Keep-Alive", "Trailer", "Transfer-Encoding", "Proxy-Connection", "X-Custom-A", "X-Custom-B", "X-Proxy-Tok"} {
		if h.Get(key) != "" {
			t.Errorf("hop-by-hop header %q survived", key)
		}
	}
	for _, key := range []string{"Authorization", "Content-Encoding", "Content-Type"} {
		if h.Get(key) == "" {
			t.Errorf("end-to-end header %q was wrongly deleted", key)
		}
	}
}

func TestBuildSyntheticResponse(t *testing.T) {
	orig := &http.Response{
		Status:     "429 Too Many Requests",
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{},
	}
	orig.Header.Set("Content-Length", "100000") // 上游声明的大值，必须被覆盖
	orig.Header.Set("Transfer-Encoding", "chunked")
	orig.Header.Set("Content-Encoding", "gzip")
	orig.Header.Set("Retry-After", "30")

	body := []byte(`{"error":{"code":"credit_balance_exhausted"}}`)
	resp := buildSyntheticResponse(orig, body)

	if got, want := resp.Header.Get("Content-Length"), strconv.Itoa(len(body)); got != want {
		t.Errorf("Content-Length = %q, want %q", got, want)
	}
	if resp.Header.Get("Transfer-Encoding") != "" {
		t.Error("Transfer-Encoding should be removed")
	}
	for _, key := range []string{"Content-Encoding", "Retry-After"} {
		if resp.Header.Get(key) == "" {
			t.Errorf("end-to-end header %q must be preserved", key)
		}
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != string(body) {
		t.Errorf("body mismatch: %q", got)
	}
}

func TestWriteJSONError(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSONError(w, http.StatusForbidden, "upstream host not allowed: evil")
	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
	var m map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if m["error"] == "" {
		t.Error("missing error field")
	}
}

func TestCopyHeaderAndMaxInt(t *testing.T) {
	src := http.Header{}
	src.Add("X-Multi", "a")
	src.Add("X-Multi", "b")
	dst := http.Header{}
	copyHeader(dst, src)
	if len(dst.Values("X-Multi")) != 2 {
		t.Error("copyHeader lost multi-value header")
	}
	if maxInt([]int{1, 9, 3}) != 9 {
		t.Error("maxInt")
	}
	if id := newRequestID(); len(id) != 8 {
		t.Errorf("request ID length = %d, want 8", len(id))
	}
}

func TestReadLimitedTruncates(t *testing.T) {
	big := strings.Repeat("x", 200<<10)
	got, err := readLimited(strings.NewReader(big), max429BodyBytes)
	if err != nil {
		t.Fatalf("readLimited: %v", err)
	}
	if len(got) != max429BodyBytes {
		t.Errorf("len = %d, want %d", len(got), max429BodyBytes)
	}
}

// ---------------------------------------------------------------------------
// 2. 配置
// ---------------------------------------------------------------------------

func TestDefaultConfigIsValid(t *testing.T) {
	if err := validateConfig(defaultConfig()); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}

func TestValidateConfigRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"non-loopback", func(c *Config) { c.Listen = "0.0.0.0:18888" }},
		{"bad listen", func(c *Config) { c.Listen = "not-an-address" }},
		{"tiny body", func(c *Config) { c.MaxBodyBytes = 100 }},
		{"cap ratio 0", func(c *Config) { c.RetryAfterCapRatio = 0 }},
		{"empty ramp", func(c *Config) { c.NormalRamp.DelaysMs = nil }},
		{"zero delay", func(c *Config) { c.NormalRamp.DelaysMs = []int{0} }},
		{"jitter > 1", func(c *Config) { c.AmbiguousRamp.JitterRatio = 1.5 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := defaultConfig()
			c.mutate(cfg)
			if err := validateConfig(cfg); err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestLoadConfig_MissingFileIsFatal(t *testing.T) {
	if _, err := loadConfig("definitely-missing-config.json"); err == nil {
		t.Error("missing config must return an error, not silently fall back to defaults")
	}
}

func TestLoadConfig_EnvOverride(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/config.json"
	if err := os.WriteFile(path, []byte(`{"listen":"127.0.0.1:19999","logLevel":"warn"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LISTEN", "localhost:17777")
	t.Setenv("LOG_LEVEL", "error")
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Listen != "localhost:17777" || cfg.LogLevel != "error" {
		t.Errorf("env override failed: listen=%q level=%q", cfg.Listen, cfg.LogLevel)
	}
	// 未出现在 JSON 里的字段保留默认值
	if cfg.MaxBodyBytes != 262144 {
		t.Errorf("defaults lost: maxBodyBytes=%d", cfg.MaxBodyBytes)
	}
}

// ---------------------------------------------------------------------------
// 15. 端到端
// ---------------------------------------------------------------------------

func TestE2E_RetryThenSuccess(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"type":"rate_limit_exceeded"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: ok\n\n"))
	}))
	defer upstream.Close()

	cfg := testConfig()
	gw := startGateway(t, cfg)
	defer gw.Close()

	resp, err := http.Post(gatewayTarget(gw, upstream, "/v1/chat/completions"),
		"application/json", strings.NewReader(`{"model":"gpt-4o-mini"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %q", resp.StatusCode, body)
	}
	if string(body) != "data: ok\n\n" {
		t.Errorf("body = %q", body)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("upstream calls = %d, want 3", got)
	}
}

func TestE2E_QuotaExhaustedPassthrough(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Retry-After", "30")
		w.Header().Set("Content-Encoding", "identity")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"type":"insufficient_quota","code":"credit_balance_exhausted"}}`))
	}))
	defer upstream.Close()

	cfg := testConfig()
	gw := startGateway(t, cfg)
	defer gw.Close()

	resp, err := http.Post(gatewayTarget(gw, upstream, "/v1/chat/completions"),
		"application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") != "30" {
		t.Error("Retry-After not preserved")
	}
	if resp.Header.Get("Content-Encoding") != "identity" {
		t.Error("Content-Encoding must survive stage 2")
	}
	if got, want := resp.Header.Get("Content-Length"), strconv.Itoa(len(body)); got != want {
		t.Errorf("Content-Length = %q, want %q", got, want)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("quota exhausted must NOT retry, got %d calls", got)
	}
}

func TestE2E_NetworkError502NoRetry(t *testing.T) {
	cfg := testConfig()
	gw := startGateway(t, cfg)
	defer gw.Close()

	// 端口 1 几乎肯定无服务；allowHttp=true 放行 http:// 前缀
	target := gw.URL + "/http://127.0.0.1:1/v1/chat/completions"
	resp, err := http.Post(target, "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %q", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "upstream request failed") {
		t.Errorf("body = %q", body)
	}
}

func TestE2E_PassthroughNoRetry(t *testing.T) {
	var mu sync.Mutex
	var gotAuth, gotTE string
	var gotCL int64
	var calls int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		gotTE = strings.Join(r.TransferEncoding, ",")
		gotCL = r.ContentLength
		mu.Unlock()
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusTooManyRequests) // 非 chat 路径不得重试
		_, _ = w.Write([]byte("raw upstream 429"))
	}))
	defer upstream.Close()

	cfg := testConfig()
	gw := startGateway(t, cfg)
	defer gw.Close()

	req, _ := http.NewRequest(http.MethodPost, gatewayTarget(gw, upstream, "/v1/models"),
		strings.NewReader(`{"x":1}`))
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusTooManyRequests || string(body) != "raw upstream 429" {
		t.Fatalf("passthrough broken: status=%d body=%q", resp.StatusCode, body)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("passthrough must not retry, got %d calls", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotAuth != "Bearer sk-test" {
		t.Errorf("Authorization not forwarded: %q", gotAuth)
	}
	if gotTE != "" {
		t.Errorf("Content-Length degraded to chunked: TE=%q CL=%d", gotTE, gotCL)
	}
	if gotCL <= 0 {
		t.Errorf("upstream saw ContentLength=%d, want >0", gotCL)
	}
}

func TestE2E_ErrorCodes(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	t.Run("400 missing prefix", func(t *testing.T) {
		gw := startGateway(t, testConfig())
		defer gw.Close()
		resp, err := http.Get(gw.URL + "/v1/models")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d", resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q", ct)
		}
	})

	t.Run("400 http upstream rejected", func(t *testing.T) {
		cfg := testConfig()
		cfg.AllowHttp = false
		gw := startGateway(t, cfg)
		defer gw.Close()
		resp, err := http.Get(gw.URL + "/http://example.com/v1/models")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d", resp.StatusCode)
		}
		if !strings.Contains(string(body), "allowHttp: true") {
			t.Errorf("error must name allowHttp: %q", body)
		}
	})

	t.Run("403 whitelist", func(t *testing.T) {
		cfg := testConfig()
		cfg.AllowedUpstreamHosts = []string{"allowed.example.com:443"}
		gw := startGateway(t, cfg)
		defer gw.Close()
		resp, err := http.Get(gatewayTarget(gw, upstream, "/v1/models"))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("status = %d", resp.StatusCode)
		}
	})

	t.Run("413 body too large", func(t *testing.T) {
		cfg := testConfig()
		cfg.MaxBodyBytes = 64
		gw := startGateway(t, cfg)
		defer gw.Close()
		resp, err := http.Post(gatewayTarget(gw, upstream, "/v1/chat/completions"),
			"application/json", strings.NewReader(strings.Repeat("x", 200)))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("status = %d", resp.StatusCode)
		}
	})
}

func TestE2E_RequestURIPreservesDoubleSlash(t *testing.T) {
	// 15.11 的 Go 客户端侧回归：确认 // 未被归一化、: 未被转义
	var seen string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	gw := startGateway(t, testConfig())
	defer gw.Close()
	resp, err := http.Get(gatewayTarget(gw, upstream, "/v1/models"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gateway returned %d — double slash may have been normalized", resp.StatusCode)
	}
	if seen != "/v1/models" {
		t.Errorf("upstream path = %q", seen)
	}
}
