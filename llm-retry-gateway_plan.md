# Plan: 本地 LLM 429 重试代理网关（Go 单文件）

## 0. 项目概述

### 0.1 目标

本地单文件 Go HTTP 代理网关。Agent 将 OpenAI 兼容 base URL 指向 `http://127.0.0.1:18888/https://upstream.host/v1`，网关剥离前缀后转发到真实上游（默认仅允许 `https://` 上游）。**仅对 `/chat/completions` 路径**执行 429 自动重试；其余路径原样透传。

**只有 HTTP 429 进入重试循环**，且重试无限次（无 `maxRetries`）。重试循环的退出条件：拿到非 429 响应（交给阶段二透传）、明确配额耗尽、客户端断开。网络错误（连接失败、`ResponseHeaderTimeout`、读错误）**不重试**，直接返回 502，避免非幂等 `POST` 被重放。

### 0.2 核心原则

1. **首字节前（仅 HTTP 429）可重试，首字节后不回头**；网络错误在首字节前也直接 502、不重试。
2. **仅 429 进入重试循环，且重试无限次**，无 `maxRetries`；网络错误不重试，直接 502。
3. **分类驱动退避**，严格优先级：`quotaExhaustedPatterns`（明确配额耗尽）→ `rateLimitPatterns`（明确瞬时限流）→ `insufficient_quota`（模糊配额，语义信号）→ `Retry-After`（时序信号）→ 未分类；语义信号优先于时序信号。
4. **`Retry-After` 是延迟下限**：先按**所选 ramp** 的 `max(delaysMs) × retryAfterCapRatio` 封顶，实际延迟 = `max(封顶后的 Retry-After, 缓坡延迟)`；缓坡在单个 ramp 周期内递增、到顶后循环（非全局单调），封顶后继续循环重试，不透传。
5. **API Key 纯透传**：网关不管理、不注入密钥，`Authorization` 头原样转发。
6. **请求体原样透传**：网关不解析模型参数；chat 路径读入内存以支持重放（受 `maxBodyBytes` 限制），透传路径直接流式转发、不缓冲。

### 0.3 不做的事 / 已知限制

- 不做 Key 池 / 轮换 / 降级
- 不做主动限流
- 不做配置热重载 / Web UI
- 不缓冲完整 SSE
- 不对 `/models` 等非 chat 端点重试
- 不修改请求体或响应体 JSON（**例外**：429 配额耗尽响应体固定截断到前 `max429BodyBytes` = 64KB，非字节级透传）
- 不管理 API Key（无环境变量注入、无兜底）
- 不重试网络错误（仅 429 触发重试）
- 默认不允许 `http://` 上游（需 `allowHttp: true` 显式开启）
- **已知限制**：只覆盖 `/chat/completions`，不覆盖新版 `/responses` API，也不匹配带尾部斜杠的 `/chat/completions/` 变体；chat 路径固定使用 `POST`（该端点只可能是 POST）。

---

## 1. 依赖与文件结构

- **Go 标准库 only**，无第三方依赖。
- **单文件** `main.go`。
- **配置文件** `config.json`（相对进程工作目录 CWD，非可执行文件目录），可通过 `-config` 指定；**文件不存在则打印错误并退出，不静默回退默认值**。

允许的 import：

```text
bytes, context, crypto/rand, encoding/json, flag, fmt, io, log, math/rand, net,
net/http, net/url, os, os/signal, strconv, strings, syscall, time
```

`crypto/rand` 与 `math/rand` 的包名同为 `rand`，**必须别名**：

```go
crand "crypto/rand"   // request ID
mrand "math/rand"     // 退避抖动 rand.Float64()
```

`min` / `max` 是 Go 1.21+ 内建函数，不要自定义；仅 `maxInt([]int)` 需要辅助函数（内建 `max` 不接受切片）。

---

## 2. 配置

### 2.1 加载顺序

1. 默认值
2. `config.json` 覆盖（`-config` 相对 CWD；**文件缺失/解析失败 → 打印错误并 `os.Exit(1)`**）
3. 环境变量覆盖：`LISTEN` → `listen`，`LOG_LEVEL` → `logLevel`

### 2.2 config.json 完整示例

```json
{
  "listen": "127.0.0.1:18888",
  "allowHttp": false,
  "allowedUpstreamHosts": [],
  "maxBodyBytes": 262144,
  "logLevel": "info",
  "normalRamp": {
    "delaysMs": [2000, 2000, 3000, 3000, 3000, 4500, 4500, 6750, 10000],
    "jitterRatio": 0.25
  },
  "ambiguousRamp": {
    "delaysMs": [4000, 6000, 9000, 13500, 20250, 30000],
    "jitterRatio": 0.25
  },
  "retryAfterCapRatio": 4,
  "quotaExhaustedPatterns": [
    "credit_balance_exhausted",
    "organization_usage_limit_exceeded",
    "organization_spend_limit_exceeded",
    "project_spend_limit_exceeded",
    "daily usage limit exceeded"
  ],
  "rateLimitPatterns": [
    "rate_limit_exceeded",
    "token_rate_limit_exceeded",
    "ip_rate_limit_exceeded",
    "concurrency_rate_limit_exceeded",
    "tpm exhausted",
    "rpm exhausted"
  ]
}
```

### 2.3 字段说明

| 字段 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `listen` | string | `"127.0.0.1:18888"` | 监听地址，**只允许回环地址** |
| `allowHttp` | bool | `false` | 是否允许 `http://` 上游；默认关闭以防 API Key 明文出网 |
| `allowedUpstreamHosts` | []string | `[]` | 上游 host 白名单，比较前两侧都做 `normalizeHost`；为空允许所有但启动打风险提示 |
| `maxBodyBytes` | int | `262144` | chat 路径请求体的内存上限（256KB）。**透传路径不缓冲请求体，不受此限制** |
| `logLevel` | string | `"info"` | `debug` / `info` / `warn` / `error` |
| `normalRamp.delaysMs` | []int | 见示例 | 正常瞬时限流/未分类的退避毫秒序列，循环使用 |
| `normalRamp.jitterRatio` | float | `0.25` | 抖动比例，范围 [0,1] |
| `ambiguousRamp.delaysMs` | []int | 见示例 | 模糊配额的退避毫秒序列，循环使用 |
| `ambiguousRamp.jitterRatio` | float | `0.25` | 同上 |
| `retryAfterCapRatio` | int | `4` | `Retry-After` 封顶比例，封顶 = **所选 ramp** 的 `max(delaysMs) × 该值`；`Retry-After` 仅作为延迟下限 |
| `quotaExhaustedPatterns` | []string | 见示例 | 明确配额耗尽子串（大小写不敏感） |
| `rateLimitPatterns` | []string | 见示例 | 明确瞬时限流子串（大小写不敏感） |

补充约定：

- **`max429BodyBytes` 是编译期常量 `64 * 1024`，不是配置项**。429 响应体读取上限与请求体上限解耦；即使 `maxBodyBytes` 调小，429 分类仍可读到 64KB。
- `maxBodyBytes` 默认 256KB：带图片 / base64 或超长上下文的 chat 请求需调大；**内存放大 ≈ 并发数 × (`maxBodyBytes` + 64KB)**（请求体重放需留在内存，429 并发另占固定 64KB）。`config.json` 是纯 JSON、不支持注释，此说明以 §2.3 为准，若后续另建 README 再同步；**不要在 JSON 示例里写注释**（会产生非法配置）。

### 2.4 启动校验

任一不满足则打印错误并 `os.Exit(1)`：

0. 配置文件存在且可解析（缺失不回退默认值）。
1. `listen` 的 host 是 `127.0.0.1` / `::1` / `localhost`（用 `net.SplitHostPort`）。
2. `maxBodyBytes >= 1024`。
3. `retryAfterCapRatio >= 1`。
4. 对 `normalRamp` 和 `ambiguousRamp`：
   - `len(delaysMs) > 0`
   - 所有值 `> 0`
   - `jitterRatio` 在 `[0, 1]`

`allowedUpstreamHosts` 为空时打印风险提示：

```text
gateway will forward to ANY host including internal network. Set it to a whitelist if this machine is shared.
```

---

## 3. 请求处理流程

### 3.1 顶层

```
handle(w, r, cfg, logger):
  1. extractUpstreamURL(r, cfg)  → u *url.URL, err
  2. 校验 scheme（含 allowHttp）、host 白名单（normalizeHost）
  3. 若 u.Path 以 "/chat/completions" 结尾 → handleChat(w, r, u, cfg, logger)
     否则 → handlePassthrough(w, r, u, cfg, logger)
  4. 错误经 writeJSONError 映射为 400 / 403
```

**必须使用裸 `http.Server` + `http.HandlerFunc` 闭包**（`handle` 需要 `cfg` / `logger`，见 §13）：

```go
srv := &http.Server{
    Addr: cfg.Listen,
    Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        handle(w, r, cfg, logger)
    }),
}
```

**禁止 `http.ServeMux`**（`cleanPath` 会合并 `//` 并 301）。

### 3.2 `handleChat`

```
handleChat(w, r, u, cfg, logger):
  ctx := r.Context()
  reqID := newRequestID()

  bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, int64(cfg.MaxBodyBytes)+1))
  if err != nil           → writeJSONError(w, 400, ...)
  if len > MaxBodyBytes   → writeJSONError(w, 413, "request body too large")

  阶段一：resp, err := forwardWithRetry(ctx, u.String(), bodyBytes, r.Header, cfg, logger, reqID)
  if err != nil:
      if ctx.Err() != nil { return }                       // 客户端已断，不写任何响应
      writeJSONError(w, 502, "upstream request failed: "+err.Error())
      return
  defer resp.Body.Close()                                  // 由 handleChat 负责关闭

  阶段二：streamResponse(w, resp, logger, reqID)
```

- `http.NewRequest` 失败同样走 502（§6 直接返回 error）。
- chat 路径固定 `POST`（§0.3 已知限制）。
- 阶段二开始后（`w.WriteHeader`）绝不重试。

### 3.3 `handlePassthrough`

```
1. 构造上游请求：req.Method = r.Method；body 直接用 r.Body（不读入内存、不受 maxBodyBytes 限制）
   req.ContentLength = r.ContentLength        // -1 时保持 chunked，那是客户端自己的选择
2. copyHeader 透传业务头；delHopByHop 删除逐跳头；按 §10.2 删除 `Content-Length` 头（由 `req.ContentLength` 统一决定）；`req.Header.Del("Host")` 让 Go 用 URL host
3. upstreamClient.Do(req)                      // 包级共享 client，含 ResponseHeaderTimeout
4. defer resp.Body.Close()
5. delHopByHop(resp.Header) 后 copyHeader + WriteHeader
6. 流式转发（复用 §8 的循环，定期 Flush）
7. 网络错误 → writeJSONError(w, 502, "upstream request failed: ...")
8. 不重试、不分类
```

---

## 4. 上游 URL 提取

```go
func extractUpstreamURL(r *http.Request, cfg *Config) (*url.URL, error) {
    raw := r.RequestURI
    if raw == "" {
        raw = r.URL.EscapedPath()
        if r.URL.RawQuery != "" { raw += "?" + r.URL.RawQuery }
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
        if !cfg.AllowHttp { return nil, errHTTPNotAllowed }
        upstream = path[1:]
    default:
        return nil, fmt.Errorf("missing upstream URL, expected path like /https://host/v1/...")
    }
    if query != "" { upstream += "?" + query }

    u, err := url.Parse(upstream)
    if err != nil { return nil, err }
    if u.Scheme != "http" && u.Scheme != "https" {
        return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
    }
    if u.Scheme == "http" && !cfg.AllowHttp { return nil, errHTTPNotAllowed }
    if u.Host == "" { return nil, fmt.Errorf("missing host in upstream URL") }

    if len(cfg.AllowedUpstreamHosts) > 0 {
        host := normalizeHost(u.Hostname())
        allowed := false
        for _, h := range cfg.AllowedUpstreamHosts {
            if host == normalizeHost(h) { allowed = true; break }
        }
        if !allowed { return nil, errForbidden(u.Hostname()) }
    }
    return u, nil
}
```

`normalizeHost` 统一比较口径（白名单项填 `api.example.com:443`、`[::1]`、`localhost:18888` 都能与上游匹配；不做通配符、不做端口白名单）：

```go
func normalizeHost(h string) string {
    h = strings.TrimSpace(strings.ToLower(h))
    if host, _, err := net.SplitHostPort(h); err == nil { h = host }
    h = strings.Trim(h, "[]")
    return strings.TrimSuffix(h, ".")
}
```

错误映射：

| 错误 | 状态码 | 响应体 |
| :--- | :--- | :--- |
| `errHTTPNotAllowed` | 400 | `{"error":"http upstream not allowed; set allowHttp: true to enable"}` |
| `errForbidden` | 403 | `{"error":"upstream host not allowed: ..."}` |
| 其他 | 400 | `{"error":"..."}` |

> **15.11 已实测**：`r.RequestURI` 在 HTTP/2 下也会保留 `//`，`EscapedPath` 分支仅为防御性回退。若将来更换 Agent/SDK 导致 `//` 被归一化，备选方案是 `X-Upstream-Base` header 传上游，网关优先读 header、回退 path；届时须同步修改 §4 / §9 / §10 / §15 / §17。

---

## 5. 429 分类器

### 5.1 类型

```go
type Classification int
const (
    ClassRateLimit Classification = iota
    ClassAmbiguousQuota
    ClassQuotaExhausted
    ClassUnclassified
)
```

### 5.2 规则

```
输入：bodyBytes []byte, headers http.Header, cfg

1. 尝试 JSON 解析，若成功，提取以下字段拼接为 haystack：
   error.type, error.code, error.message, message, detail（字符串时）, code（字符串时）
   若解析失败，haystack = string(bodyBytes)
2. haystack = strings.ToLower(haystack)
3. 若任一 quotaExhaustedPatterns（小写）出现在 haystack → ClassQuotaExhausted
4. 若任一 rateLimitPatterns（小写）出现在 haystack → ClassRateLimit
5. 若 haystack 包含 "insufficient_quota" → ClassAmbiguousQuota
6. 若 Retry-After 头可解析为有效时长（parseRetryAfter > 0）→ ClassRateLimit
7. 否则 → ClassUnclassified
```

顺序依据：`quotaExhaustedPatterns` 优先于 `rateLimitPatterns`；**`insufficient_quota` 是语义信号，`Retry-After` 是时序信号，语义优先**，因此把 `insufficient_quota` 放在 `Retry-After` 之前。调整后 `Retry-After` 的信息不丢——它仍参与延迟计算，只是作为下限（§6 / §7.3），退避序列改从 `ambiguousRamp` 取。

**禁止匹配裸数字 "429"**。

**压缩 429 的已知限制**：`copyHeader` 会原样转发客户端的 `Accept-Encoding`；Go Transport 只在请求**没有** `Accept-Encoding` 时才自动加 gzip 并透明解压，所以网关实际走的是「不自动解压」路径。若上游返回 `Content-Encoding: gzip` 的 429，`bodySnippet` 是压缩字节，JSON 解析与子串匹配都会失效 → 退化为 `ClassUnclassified`（走 `normalRamp`）。这是已知限制，不影响重试循环本身；需要覆盖时可在转发请求里删除 `Accept-Encoding` 让 Transport 自动处理。**响应头的 `Content-Encoding` 必须保留**（见 §6.1），否则客户端会把 gzip 字节当明文解析。

### 5.3 读取限制

```go
func readLimited(body io.Reader, limit int) ([]byte, error) {
    buf := make([]byte, 0, 1024)
    tmp := make([]byte, 1024)
    for len(buf) < limit {
        n, err := body.Read(tmp[:min(len(tmp), limit-len(buf))])
        if n > 0 { buf = append(buf, tmp[:n]...) }
        if err == io.EOF { break }
        if err != nil { return buf, err }
    }
    return buf, nil
}
```

- 429 响应体统一用 `readLimited(resp.Body, max429BodyBytes)`（固定 64KB，与 `maxBodyBytes` 解耦）。
- **调用后必须 `resp.Body.Close()`**（在 `forwardWithRetry` 内完成，§6）。截断时是**提前关闭**（未读到 EOF），该连接不会归还共享池；对 429 错误响应可接受，若要复用需先 drain 剩余 body。
- `min` 用内建函数，不自定义。

---

## 6. 重试循环

**包级共享 client**：不要每请求新建 `http.Transport`，否则每个请求都是一个新连接池。

```go
var upstreamClient = newHTTPClient()

func newHTTPClient() *http.Client {
    tr := http.DefaultTransport.(*http.Transport).Clone()
    tr.ResponseHeaderTimeout = 60 * time.Second   // 建连后 60s 不发响应头 → 网络错误
    return &http.Client{
        Transport: tr,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
}
```

```go
func forwardWithRetry(ctx context.Context, upstreamURL string, bodyBytes []byte,
                      originalHeader http.Header, cfg *Config, logger *Logger,
                      reqID string) (*http.Response, error) {

    attempt := 0
    rampIndex := 0

    for {
        attempt++
        if ctx.Err() != nil { return nil, ctx.Err() }

        req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL,
                                                bytes.NewReader(bodyBytes))
        if err != nil { return nil, err }

        copyHeader(req.Header, originalHeader)
        req.Header.Del("Host")
        req.Header.Del("Content-Length")     // 由 req.ContentLength 决定，见 §10.2
        delHopByHop(req.Header)
        // Authorization 已在 originalHeader 中，copyHeader 已透传

        resp, err := upstreamClient.Do(req)
        if ctx.Err() != nil {
            if resp != nil { resp.Body.Close() }
            return nil, ctx.Err()
        }

        if err != nil {
            // 仅 429 重试；网络错误不重放非幂等 POST，直接返回 error
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
                        time.Duration(cfg.RetryAfterCapRatio)   // 不命名为 cap，避免遮蔽内建
            capped = ra
            if capped > capDelay { capped = capDelay }
        }
        rampDelay := computeRampDelay(ramp, rampIndex)
        delay := max(capped, rampDelay)       // Retry-After 是下限；max 为 Go 1.21+ 内建
        rampIndex++                           // 无论是否使用 Retry-After 都递增

        logger.Info("req=%s attempt=%d sleep_ms=%d before_attempt=%d",
                    reqID, attempt, delay.Milliseconds(), attempt+1)
        if !sleepWithContext(ctx, delay) {
            return nil, ctx.Err()
        }
    }
}
```

`lastClass` 与网络错误退避分支已删除（只有 429 会走到退避计算）。

### 6.1 `buildSyntheticResponse`

原 body 已被读取，必须重建；**Header 是 `streamResponse` 唯一读取的来源**，结构体 `ContentLength` 字段不会被用到：

```go
func buildSyntheticResponse(orig *http.Response, bodyBytes []byte) *http.Response {
    h := orig.Header.Clone()
    h.Del("Transfer-Encoding")
    h.Set("Content-Length", strconv.Itoa(len(bodyBytes)))   // body 可能被 64KB 截断
    return &http.Response{
        Status: orig.Status, StatusCode: orig.StatusCode,
        Proto: orig.Proto, ProtoMajor: orig.ProtoMajor, ProtoMinor: orig.ProtoMinor,
        Header: h,
        Body: io.NopCloser(bytes.NewReader(bodyBytes)),
        ContentLength: int64(len(bodyBytes)),
        Request: orig.Request,
    }
}
```

保留 `Retry-After` 头让 Agent 读到原始值；`Set` 是**覆盖**上游可能残留的大 `Content-Length`（64KB 截断时实测 `100000 → 65536`），避免声明长度与实际写出不一致导致客户端挂住。**只动 `Content-Length` 和 `Transfer-Encoding`，不要删 `Content-Encoding` / `Content-Type`**（见 §5.2 压缩 429 说明）。

### 6.2 `sleepWithContext`

```go
func sleepWithContext(ctx context.Context, d time.Duration) bool {
    if d <= 0 { return true }
    t := time.NewTimer(d)
    defer t.Stop()
    select {
    case <-ctx.Done(): return false
    case <-t.C: return true
    }
}
```

### 6.3 `rampFor`

```go
func rampFor(class Classification, cfg *Config) *RampConfig {
    if class == ClassAmbiguousQuota { return &cfg.AmbiguousRamp }
    return &cfg.NormalRamp
}
```

---

## 7. 退避计算

### 7.1 `computeRampDelay`

```go
func computeRampDelay(ramp *RampConfig, index int) time.Duration {
    v := ramp.DelaysMs[index % len(ramp.DelaysMs)]
    jitter := float64(v) * ramp.JitterRatio
    actual := float64(v) - jitter + mrand.Float64()*2*jitter
    if actual < 0 { actual = 0 }
    return time.Duration(actual) * time.Millisecond
}
```

`index` 在**每次计算 ramp 退避后递增**（请求级恒增，不因分类切换重置，**也不因使用 `Retry-After` 暂停**）。注意 ramp 用 `index % len(delaysMs)` 取模，延迟在**单个周期内递增、到顶后回绕循环**，不是全局单调。

### 7.2 `parseRetryAfter`

```go
func parseRetryAfter(v string) time.Duration {
    v = strings.TrimSpace(v)
    if v == "" { return 0 }
    if sec, err := strconv.Atoi(v); err == nil && sec >= 0 {
        return time.Duration(sec) * time.Second
    }
    if t, err := http.ParseTime(v); err == nil {
        d := time.Until(t)
        if d < 0 { d = 0 }
        return d
    }
    return 0
}
```

### 7.3 `Retry-After` 封顶与下限

```
retryAfterCapped = min(parseRetryAfter(Retry-After), cap)
cap              = 所选 ramp 的 max(delaysMs) × retryAfterCapRatio
rampDelay        = computeRampDelay(ramp, rampIndex)
delay            = max(retryAfterCapped, rampDelay)
rampIndex++       // 恒递增
```

- 正常 ramp：`10000 × 4 = 40s`；模糊 ramp：`30000 × 4 = 120s`。分类为 `ClassAmbiguousQuota` 时用模糊 ramp 的 120s 上限。
- 语义：**`Retry-After` 是延迟下限，缓坡在单个 ramp 周期内递增、到顶后循环**（非全局单调）；封顶后继续重试，不透传。
- 例：上游持续返回 `Retry-After: 1`，延迟由 `normalRamp` 决定，从约 2s 爬到 10s 后回绕循环（不会永远 1s 一次）；上游返回 `Retry-After: 40s` 时仍为 40s（缓坡 ≤ 10s，不覆盖下限）。
- 超过封顶的 `Retry-After` 会被压到封顶：`normalRamp` 下 `Retry-After: 60s` → 40s，`ambiguousRamp` 下 → 120s。
- **两种典型行为**：
  - 持续 `Retry-After: 60s`（normalRamp）：`retryAfterCapped = min(60s, 40s) = 40s`，而 `rampDelay` 始终在 2s~10s 之间循环、恒小于 40s，故 `delay` **恒为 40s**——缓坡回绕完全不体现，实际表现为「被 `Retry-After` 压住」的恒定退避。
  - 持续 `Retry-After: 1`：`delay` 序列为 `2s, 2s, 3s, 3s, 3s, 4.5s, 4.5s, 6.75s, 10s, 2s, ...`，单周期内递增、到顶回绕。这是锯齿语义真正生效的场景。
  - 即：**只有 `Retry-After` 封顶值落进缓坡取值区间时，缓坡回绕才会体现在实际延迟上**。

---

## 8. 阶段二：流式透传

```go
func streamResponse(w http.ResponseWriter, resp *http.Response, logger *Logger, reqID string) {
    delHopByHop(resp.Header)
    copyHeader(w.Header(), resp.Header)
    w.WriteHeader(resp.StatusCode)

    flusher, _ := w.(http.Flusher)
    buf := make([]byte, 32*1024)
    for {
        n, err := resp.Body.Read(buf)
        if n > 0 {
            if _, werr := w.Write(buf[:n]); werr != nil { return }
            if flusher != nil { flusher.Flush() }
        }
        if err != nil {
            if err != io.EOF { logger.Warn("req=%s stream copy error: %v", reqID, err) }
            return
        }
    }
}
```

- `resp.Body.Close()` 由调用方（`handleChat` / `handlePassthrough`）`defer` 负责，`streamResponse` 不关闭。
- `w.WriteHeader` 之后**绝不重试**。
- 流中断只记日志并结束。
- **`Content-Encoding` / `Content-Type` 是端到端头，必须原样透传，绝不能删**。它们不在 §10.1 的 hop-by-hop 固定表里，`delHopByHop` 不得触及；上游返回 gzip 压缩响应时，客户端要靠 `Content-Encoding` 才能解压。实现时不要因为「修 `Content-Length`」而连带删除它们——`buildSyntheticResponse` 只动 `Content-Length` 和 `Transfer-Encoding`（§6.1）。理由与压缩 429 的影响见 §5.2。

---

## 9. Authorization 处理

**纯透传，网关不注入任何 Key。**

```go
// 在 forwardWithRetry 和 handlePassthrough 中，均通过 copyHeader 原样复制
// originalHeader 里的 Authorization，不需要额外逻辑。
```

- 请求带 `Authorization` → 原样转发。
- 请求不带 `Authorization` → 不添加，上游返回 401，原样透传。
- 无环境变量、无兜底、无 `apiKeyEnv` 配置项。

若启用 `X-Upstream-Base` header 备选方案（§4），该 header 由网关消费，不转发给上游。

---

## 10. Header 处理

### 10.1 hop-by-hop

```go
var hopByHopHeaders = []string{
    "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
    "Te", "Trailer", "Transfer-Encoding", "Upgrade", "Proxy-Connection",
}

// 必须在删除 Connection / Proxy-Connection 之前先展开其 token（RFC 7230 §6.1）
// 必须用 Values 而非 Get：Get 只返回第一行，多个 Connection 行会漏删。
func delHopByHop(h http.Header) {
    for _, key := range []string{"Connection", "Proxy-Connection"} {
        for _, v := range h.Values(key) {
            for _, token := range strings.Split(v, ",") {
                if t := strings.TrimSpace(token); t != "" { h.Del(t) }
            }
        }
    }
    for _, k := range hopByHopHeaders { h.Del(k) }
}
```

### 10.2 请求

1. `copyHeader(req.Header, originalHeader)` — 透传业务头（含 `Authorization`）
2. `req.Header.Del("Host")` — 让 Go 用 URL host
3. `delHopByHop(req.Header)` — 固定 9 项（含 `"Trailer"` / `Proxy-Connection`）+ `Connection` / `Proxy-Connection` token 展开
4. `req.Header.Del("Content-Length")` — 统一由 `req.ContentLength` 决定：
   - **chat 路径**：`bytes.NewReader(bodyBytes)` 自动设置 `req.ContentLength`，不要手动复制该头。
   - **透传路径**：`req.ContentLength = r.ContentLength`；`-1` 时保持 chunked（客户端自己的选择）。实测：`0` 且 body 非 nil 也会退化为 chunked，所以显式赋值不可省。

### 10.3 响应

阶段二前 `delHopByHop(resp.Header)`，再 `copyHeader`。

**端到端头保留**（不在 §10.1 固定 9 项内，`delHopByHop` 不触及）：`Content-Encoding`、`Content-Type`、`Retry-After` 必须原样透传，不得删除。`Content-Encoding` 尤其关键——上游 gzip 压缩的响应必须把它透传给客户端，否则客户端会把 gzip 字节当明文解析。`Content-Length` 在响应侧同样不删，但配额耗尽时会由 `buildSyntheticResponse` 按截断后长度 `Set` **覆盖**（§6.1）；这是覆盖而非删除，二者不可混淆。

---

## 11. 日志

### 11.1 级别

```go
type LogLevel int
const (LevelDebug LogLevel = iota; LevelInfo; LevelWarn; LevelError)
type Logger struct { level LogLevel }
func (l *Logger) Info(f string, a ...any) { if l.level <= LevelInfo { log.Printf("[INFO]  "+f, a...) } }
// Debug / Warn / Error 同理
```

### 11.2 request ID

```go
func newRequestID() string {
    b := make([]byte, 4)
    crand.Read(b)
    return fmt.Sprintf("%x", b)
}
```

### 11.3 关键日志字段

`req`、`attempt`、`upstream`、`status`、`class`、`retry_after`、`sleep_ms`、`final`（`final=ok|passthrough|502`）。

---

## 12. 错误响应

统一走 `writeJSONError`，保证 `Content-Type`：

```go
func writeJSONError(w http.ResponseWriter, code int, msg string) {
    w.Header().Set("Content-Type", "application/json; charset=utf-8")
    w.WriteHeader(code)
    _ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
```

| 场景 | 状态码 | 响应体 |
| :--- | :--- | :--- |
| 缺少上游 URL 前缀 / 解析失败 / scheme 非法 | 400 | `{"error":"..."}` |
| `http://` 上游且 `allowHttp: false` | 400 | `{"error":"http upstream not allowed; set allowHttp: true to enable"}` |
| host 不在白名单 | 403 | `{"error":"upstream host not allowed: ..."}` |
| 请求体超过 `maxBodyBytes` | 413 | `{"error":"request body too large"}` |
| 读请求体失败 | 400 | `{"error":"..."}` |
| 透传路径上游失败 | 502 | `{"error":"upstream request failed: ..."}` |
| chat 路径网络错误（连接失败 / `ResponseHeaderTimeout` / 读错误） | 502 | `{"error":"upstream request failed: ..."}`，**不重试** |
| chat 路径 `http.NewRequest` 失败 | 502 | `{"error":"..."}` |
| 客户端断开（ctx 取消） | 不写响应，静默返回 | — |
| 明确配额耗尽 | 透传上游 429 | 上游 429 body 的**前 64KB 截断副本**（非字节级原样），`Content-Length` 按截断后长度重写，保留 `Retry-After` / `Content-Encoding` / `Content-Type` |

---

## 13. 启动与关闭

```go
func main() {
    configPath := flag.String("config", "config.json", "path to config file (relative to CWD)")
    flag.Parse()

    cfg, err := loadConfig(*configPath)   // 文件不存在 → error
    if err != nil { log.Fatalf("config error: %v", err) }

    logger := &Logger{level: parseLogLevel(cfg.LogLevel)}

    if len(cfg.AllowedUpstreamHosts) == 0 {
        logger.Warn("allowedUpstreamHosts is empty; gateway will forward to ANY host including internal network. Set it to a whitelist if this machine is shared.")
    }

    srv := &http.Server{
        Addr:    cfg.Listen,
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
```

不在 goroutine 内 `log.Fatalf`（会跳过优雅关闭）；`signal.Notify(SIGINT, SIGTERM)` 在 windows/amd64 可编译，Ctrl+C 走 `SIGINT`。

---

## 14. 易错点清单

1. **禁止 `http.ServeMux`**，用裸 `http.Server`。
2. **优先 `r.RequestURI`**，HTTP/2 回退 `EscapedPath`（防御性；实测 HTTP/2 下 `r.RequestURI` 已保留 `//`）。
3. **429 body 读完必须 `Close`**；**成功响应的 `resp.Body` 由 `handleChat` / `handlePassthrough` `defer Close`**（提前 return 路径必然泄漏，HTTP/2 还需 Close 才能取消 stream）。
4. **配额耗尽返回 `buildSyntheticResponse`**，保留 `Retry-After`，**并修正 `Content-Length` / 删除 `Transfer-Encoding`**。
5. **`Retry-After` 是延迟下限**（不是「优先」）：`delay = max(retryAfterCapped, rampDelay)`；封顶后继续重试，不透传。
6. **`rampIndex` 恒递增**（不因使用 `Retry-After` 暂停），请求内恒增、不因分类切换重置；但 ramp 取模循环，延迟不是全局单调。
7. **chat 路径请求体读入内存**（需重放，受 `maxBodyBytes` 限制）；透传路径直接用 `r.Body` 且**显式设置 `req.ContentLength`**。
8. **阶段二开始后不重试**。
9. **`Content-Length` 由 `req.ContentLength` 决定**：chat 路径 `bytes.NewReader` 自动设置；透传路径必须显式赋值（`-1` 保持 chunked）；不要手动从原始请求复制该头。
10. **删除 `Host` 头**，让 Go 用 URL host。
11. **`CheckRedirect` 返回 `http.ErrUseLastResponse`**。
12. **`maxDelayMs` 从所选 ramp 的 `max(delaysMs)` 计算**，不是配置项。
13. **API Key 纯透传**，无注入、无环境变量、无兜底。
14. **分类器覆盖多字段**：`error.type/code/message`、顶层 `message/code/detail`。
15. **匹配大小写不敏感子串**，不用正则；**禁止匹配裸数字 `429`**。
16. **分类顺序**：`quotaExhaustedPatterns` → `rateLimitPatterns` → `insufficient_quota` → `Retry-After` → `unclassified`；语义信号优先于时序信号。
17. **JSON 解析失败时对整个 body 做字符串匹配**。
18. **仅 429 进入重试循环**；网络错误 / `ResponseHeaderTimeout` 直接 502，不重放非幂等 `POST`。
19. **429 响应体用固定 `max429BodyBytes`（64KB）**，与 `maxBodyBytes` 解耦。
20. **`delHopByHop` 先用 `Values` 展开 `Connection` / `Proxy-Connection` 的 token，再删固定 9 项**；固定表用 `"Trailer"`（单数，非 `"Trailers"`）并含 `Proxy-Connection`；请求侧和响应侧都要调用。
21. **默认只允许 `https://` 上游**；`http://` 返回含 `allowHttp` 配置项名的 400。
22. **白名单比较前两侧都做 `normalizeHost`**（去端口 / 方括号 / 尾点 / 大小写）。
23. **`crypto/rand` 与 `math/rand` 包名冲突**，必须别名 `crand` / `mrand`。
24. **共享包级 `http.Client`**（含 `ResponseHeaderTimeout: 60s`），不要每请求新建 `Transport`（否则每请求一个新连接池）。
25. **`min` / `max` 是 Go 1.21+ 内建**，不要自定义；只有 `maxInt([]int)` 需要辅助函数。
26. **`config.json` 缺失直接报错退出**，不静默回退默认值；`-config` 相对 CWD。
27. **`errCh` + 主 goroutine `select` 关闭**，不在 goroutine 里 `log.Fatalf`。

---

## 15. 测试场景

> **mock 前置条件（适用于 15.3–15.8、15.12、15.13 全部本地 mock）**：默认 `allowHttp: false` 只放行 `https://` 上游，因此本地 mock 必须用 `httptest.NewTLSServer`（`https://mock/...`），或临时在 config 里设 `allowHttp: true` 后用 `http://` mock。每个用例都需先满足此前提，否则会在 §4 被拦成 400 而不是进入被测逻辑。

### 15.1 URL 提取

```bash
curl -v http://127.0.0.1:18888/https://httpbin.org/get          # 成功（https 上游）
curl -v http://127.0.0.1:18888/v1/models                        # 400
curl -v http://127.0.0.1:18888/ftp://example.com/               # 400
curl -v http://127.0.0.1:18888/http://httpbin.org/get           # 400 allowHttp=false
```

> 15.1 / 15.2 的上游是 `https://httpbin.org`（`http://127.0.0.1:18888/...` 是**网关**地址），`allowHttp: false` 不影响它们。要测 `http://` 上游需临时设 `allowHttp: true` 或改用 `httptest.NewTLSServer`。

### 15.2 非 chat 透传

```bash
curl -v http://127.0.0.1:18888/https://httpbin.org/get
# 原样返回，不进重试；请求体直接流式转发，Content-Length 不退化成 chunked
```

### 15.3 429 重试

mock 上游返回 429 两次（带 `Retry-After: 1`）后返回 200 SSE。验证约 **4 秒**后收到 SSE：每次退避由 `normalRamp` 决定（`2000ms ± jitter`），`delay = max(1s, ~2s) ≈ 2s`，`Retry-After: 1` 作为下限不改变结果。mock **必须返回 429 而非网络错误**——G 下网络错误会立即 502，不进入重试循环（见 §15.12）。

### 15.4 配额耗尽

mock 返回：

```json
{"error":{"type":"insufficient_quota","code":"credit_balance_exhausted"}}
```

验证立即透传 429，日志 `quota_exhausted final=passthrough`，响应保留 `Retry-After` 且 `Content-Length` 与实际 body 一致。即使 mock 同时带 `Retry-After` 也应立即透传（`quotaExhaustedPatterns` 优先级最高）。

### 15.5 模糊配额

mock 返回：

```json
{"error":{"type":"insufficient_quota","message":"quota issue"}}
```

**mock 应同时带 `Retry-After: 1`**。验证：① 分类为 `ClassAmbiguousQuota`（语义优先于时序）；② 退避取 `ambiguousRamp`（4000ms 起）而非 `normalRamp`；③ `delay = max(1s, 4000ms ± jitter) ≈ 4s`。

### 15.6 客户端取消

```bash
timeout 3 curl http://127.0.0.1:18888/https://mock/v1/chat/completions ...
```

mock **持续返回 429**（不是网络错误——G 下网络错误会立即 502 并写下响应，覆盖不到 ctx 取消路径）。验证 3 秒后网关日志显示 ctx canceled，`forwardWithRetry` 返回 error 时因 `ctx.Err() != nil` 直接 return，**不写任何响应**、不 panic。`forwardWithRetry` 返回 error 的另一分支（网络错误 → 502）由 §15.12 覆盖，两处交叉引用，验收时不可漏测。

### 15.7 流式不缓冲

mock 每 200ms 发一个 SSE chunk，验证客户端逐块收到。

### 15.8 API Key 透传

```bash
curl -H "Authorization: Bearer sk-test" http://127.0.0.1:18888/https://mock/v1/chat/completions ...
```

验证 mock 收到 `Authorization: Bearer sk-test`。不带 Authorization 时 mock 收到空，网关不注入任何值。

### 15.9 错误响应与 `writeJSONError`

逐一触发 400（缺前缀）、403（白名单）、413（超 `maxBodyBytes`）、502（上游失败），验证响应体均为 `{"error":...}` 且 `Content-Type: application/json; charset=utf-8`。

### 15.10 `allowHttp` 与 `normalizeHost`

- `allowHttp: false` 时 `/http://host/v1/...` → 400，body 含配置项名 `allowHttp`。
- 白名单填 `api.example.com:443` / `[::1]` / `localhost:18888` 时，与 `api.example.com` / `::1` / `localhost` 的上游 host 均能匹配（`normalizeHost`）。

### 15.11 真实 SDK 验证（**实现前必做；本次已实测 PASS**）

该门槛已在本次修订中执行完毕（结论 PASS，记录见下）；保留本节作为**更换 Agent/SDK 时的回归用例**，不是待办项。

在 `handle` 临时打印 `r.RequestURI`，分别用两个官方 SDK 配 base URL，确认网关看到的是 `/https://api.openai.com/v1/chat/completions`：

```bash
# openai-python
python - <<'PY'
from openai import OpenAI
c = OpenAI(base_url="http://127.0.0.1:18888/https://api.openai.com/v1",
           api_key="sk-test", max_retries=0, timeout=5)
c.chat.completions.create(model="gpt-4o-mini", messages=[{"role": "user", "content": "hi"}])
PY

# openai-node
node -e 'const {OpenAI}=require("openai");
const c=new OpenAI({baseURL:"http://127.0.0.1:18888/https://api.openai.com/v1",apiKey:"sk-test",maxRetries:0,timeout:5000});
c.chat.completions.create({model:"gpt-4o-mini",messages:[{role:"user",content:"hi"}]});'
```

**已实测结论（2026-11-05，PASS，两个 SDK 各起本地 server 抓取真实 HTTP 请求行）**：openai-python 3.46.0（自带 vendored `httpx2` 2.16.0；独立安装的 `httpx` 0.28.1 结果一致）与 openai-node 7.51.0 / Node v26.13.0 均发送 `/https://api.openai.com/v1/chat/completions`，`//` 未合并、`:` 是字面 `0x3A` 而非 `%3A`。原因：两者都走原始字符串 / raw bytes 拼接，而非 `urljoin` 或绝对路径解析——

- openai-python `BaseClient._prepare_url`：`merge_raw_path = self.base_url.raw_path + merge_url.raw_path.lstrip(b"/")`，再 `copy_with(raw_path=...)`（`_enforce_trailing_slash` 只给 base 追加尾 `/`，不影响结果）。
- openai-node `buildURL`：`new URL(baseURL + path)`（字符串拼接后再解析）。

注意 `urllib.parse.urljoin` / `httpx.URL.join()` 会把 `//` 折叠成 `/`、`new URL("/chat/completions", base)` 会丢弃整个前缀——**测试用例本身也不可用这些 API 拼 URL**；**更换 Agent/SDK 时必须用本用例复验**。若被归一化，启用 §4 的 `X-Upstream-Base` header 备选方案。

### 15.12 网络错误不重试

mock 上游直接断连，或建连后 60s 不发响应头（触发 `ResponseHeaderTimeout`）。验证网关返回 502 `{"error":"upstream request failed: ..."}`、日志 `final=502`，且 `attempt` 只出现 1 次（无重放）。

### 15.13 `Retry-After` 下限 / livelock 回归

mock **持续**返回 429 且恒带 `Retry-After: 1`。验证日志里的 `sleep_ms` 按 `normalRamp` 从约 2000 递增到 10000 后循环，而非恒定 1000；`rampIndex` 每次都递增。

---

## 16. 代码结构

```text
1. imports（含 crand "crypto/rand"、mrand "math/rand" 别名）
2. 常量：hopByHopHeaders、max429BodyBytes = 64*1024、错误消息
3. 类型：Config, RampConfig, Classification, Logger, LogLevel
4. 包级 var upstreamClient = newHTTPClient()（含 ResponseHeaderTimeout: 60s、CheckRedirect）
5. main() + loadConfig() + validateConfig() + errCh/select 优雅关闭
6. handle(w,r,cfg,logger) + extractUpstreamURL(r,cfg) + normalizeHost()
   + handleChat() + handlePassthrough()
7. forwardWithRetry() + buildSyntheticResponse()
8. classify() + extractTextForMatching() + readLimited()
9. computeRampDelay() + parseRetryAfter() + sleepWithContext() + rampFor()
10. streamResponse(w,resp,logger,reqID) + copyHeader() + delHopByHop() + writeJSONError()
11. newRequestID() + 小工具 maxInt（min/max 用内建，不自定义）
```

---

## 17. 验收清单

- [ ] 裸 `http.Server`，未用 `ServeMux`
- [ ] `r.RequestURI` 优先，HTTP/2 回退（防御性）
- [ ] `/https://host/...` 正确剥离解析；`http://` 默认被拒且错误消息含 `allowHttp`
- [ ] 非 chat 路径原样透传不重试，method 用 `r.Method`，`req.ContentLength = r.ContentLength`
- [ ] chat 路径请求体读入内存，`maxBodyBytes` 限制；透传路径不缓冲请求体
- [ ] 429 body 读后 `Close`；成功响应 `resp.Body` 由调用方 `defer Close`
- [ ] 分类顺序 `quota → rateLimit → insufficient_quota → Retry-After → unclassified`
- [ ] 配额耗尽立即透传，保留 `Retry-After`，`Content-Length` 覆盖为截断后长度、删除 `Transfer-Encoding`，**不删 `Content-Encoding` / `Content-Type`**；429 body 是前 64KB 截断副本
- [ ] 正常序列 `2000,2000,3000,3000,3000,4500,4500,6750,10000` 循环
- [ ] 模糊序列 `4000,6000,9000,13500,20250,30000` 循环
- [ ] `Retry-After` 作为**下限**，封顶 = 所选 ramp 的 `max(delaysMs) × 4`，`delay = max(capped, rampDelay)`，封顶后继续重试
- [ ] `rampIndex` 恒递增（含使用 `Retry-After` 时），请求级恒增；ramp 取模循环、延迟非全局单调
- [ ] 无 `maxRetries`；退出条件为 ctx 取消 / 明确配额耗尽 / 非 429 响应，网络错误另走 502
- [ ] **仅 429 重试**；网络错误、`ResponseHeaderTimeout` 直接 502，不重放 `POST`
- [ ] `sleepWithContext` 支持取消；客户端断开时静默返回、不写响应、不 panic
- [ ] 阶段二不重试；`Flusher.Flush()` 流式
- [ ] **API Key 纯透传，无任何注入逻辑**
- [ ] **无 `apiKeyEnv` 配置项，无启动警告**
- [ ] 请求体模型参数原样透传，不解析
- [ ] `handleChat` 处理 `forwardWithRetry` 的 error（ctx 取消静默 / 否则 502）
- [ ] `writeJSONError` 统一错误响应，含 `Content-Type: application/json; charset=utf-8`
- [ ] `delHopByHop` 请求侧 + 响应侧都调用；`Values` 展开 `Connection`/`Proxy-Connection` token；固定表含 `"Trailer"` 与 `Proxy-Connection`
- [ ] `normalizeHost` 白名单比较口径（去端口/方括号/尾点/大小写）
- [ ] `crypto/rand`（`crand`）生成 request ID，`math/rand`（`mrand`）仅用于抖动；无自定义 `min`
- [ ] 共享包级 `upstreamClient`（`forwardWithRetry` 与 `handlePassthrough` 共用，每请求不 `Clone()`），`ResponseHeaderTimeout: 60s`，`CheckRedirect` 返回 `http.ErrUseLastResponse`
- [ ] `max429BodyBytes = 64 * 1024` 固定常量，与 `maxBodyBytes` 解耦
- [ ] `streamResponse` 用带 req ID 的 `logger.Warn`，不裸用 `log.Printf`
- [ ] `config.json` 相对 CWD，缺失即报错退出
- [ ] 启动校验全部约束（含配置文件存在）
- [ ] 非回环拒绝启动
- [ ] host 白名单为空打升级后的风险提示
- [ ] `errCh`（缓冲 1）+ `select` 关闭，`srv.Shutdown` 返回值已检查，goroutine 内无 `log.Fatalf`
- [ ] 日志含 req ID、attempt、upstream、class、sleep_ms、final
- [ ] Ramp 配置为直接毫秒数组，无 `baseDelayMs`/`rampRatio`/`repeats`
- [ ] `Content-Encoding` / `Content-Type` / `Retry-After` 作为端到端头在阶段二（含合成 429 响应）原样透传，不被 `delHopByHop` 或 `buildSyntheticResponse` 删除；`Content-Length` 只 `Set` 覆盖、不删
- [ ] 压缩 429（`Content-Encoding: gzip`）已知限制已在 §5.2 声明；§10.3 端到端头保留清单已落实
- [ ] 本地 mock 前置条件已满足（TLS mock 或 `allowHttp: true`）
- [ ] 15.11 真实 SDK 验证通过（两个 SDK 均保留 `//`），并保留为更换 SDK 时的回归用例
- [ ] 15.9–15.13 全部用例通过（错误响应、`allowHttp`/`normalizeHost`、SDK、网络错误不重试、livelock 回归）
