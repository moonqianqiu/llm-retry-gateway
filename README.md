# llm-retry-gateway

本地单文件 Go HTTP 代理网关。把 OpenAI 兼容客户端的 base URL 指向网关，网关从请求路径里剥出真实上游地址并转发；**仅对 `/chat/completions` 的 HTTP 429 做无限自动重试**。

```text
Agent 的 base_url = http://127.0.0.1:18888/https://api.openai.com/v1
                     └─ 网关监听 ─┘ └──── 真实上游 ────┘
实际请求路径       = /https://api.openai.com/v1/chat/completions
```

Go 标准库 only，单文件 `main.go`，无第三方依赖，不管理任何 API Key。

---

## 特性

- **只有 429 重试**，无限次，无 `maxRetries`；网络错误不重放非幂等 `POST`，直接 502。
- **首字节前可重试，首字节后不回头**：`WriteHeader` 之后只做流式透传，绝不重试。
- **分类驱动退避**：明确配额耗尽 / 明确瞬时限流 / 模糊配额 / 未分类，四类走不同 ramp。
- **`Retry-After` 是延迟下限**（不是「优先使用」）：`delay = max(封顶后的 Retry-After, 缓坡延迟)`，缓坡保证退避不会永远卡在 1 秒。
- **API Key 纯透传**：`Authorization` 原样转发，不注入、不兜底、不读环境变量。
- **请求体原样透传**：不解析模型参数；chat 路径读入内存以支持重放，其余路径直接流式转发。
- **默认只放行 `https://` 上游 + 只监听回环地址**，避免 Key 明文出网。

---

## 快速开始

### 构建与运行

```bash
go build -o llm-retry-gateway.exe .
./llm-retry-gateway -config config.json
```

Windows 下也可直接运行 `build.bat`（先 `go vet ./...`，再带 `-trimpath -ldflags "-s -w"` 构建）。

`-config` 默认 `config.json`，**相对进程工作目录 CWD**；文件不存在或无法解析会打印错误并退出，**不会静默回退默认值**。

### 配置 Agent

以官方 SDK 为例（把 `base_url` / `baseURL` 指向网关，上游地址写在路径里）：

```python
# openai-python
from openai import OpenAI
client = OpenAI(
    base_url="http://127.0.0.1:18888/https://api.openai.com/v1",
    api_key="sk-...",          # 原样透传给上游
)
```

```javascript
// openai-node
import OpenAI from "openai";
const client = new OpenAI({
  baseURL: "http://127.0.0.1:18888/https://api.openai.com/v1",
  apiKey: "sk-...",
});
```

> **已实测（2026-11-05）**：`openai-python 3.46.0`（自带 vendored `httpx2 2.16.0`，独立 `httpx 0.28.1` 结果一致）与 `openai-node 7.51.0` 均发送 `/https://api.openai.com/v1/chat/completions`——`//` 未合并、`:` 是字面 `0x3A` 而非 `%3A`。原因：两者都走原始字符串 / raw bytes 拼接（`BaseClient._prepare_url` 的 `raw_path` 拼接、`buildURL` 的 `new URL(baseURL + path)`），不使用 `urljoin` 或绝对路径解析。更换 Agent/SDK 后请复验，不要改用 `urllib.parse.urljoin` / `httpx.URL.join()` / `new URL(path, base)` 来拼测试 URL（它们会折叠 `//` 或丢弃前缀）。

---

## 工作原理

### 1. URL 提取

网关用裸 `http.Server` + `http.HandlerFunc`（**禁止 `http.ServeMux`**，它的路径清理会合并 `//` 并 301）。优先取 `r.RequestURI` 保留原始路径，HTTP/2 下 `EscapedPath()` 作为防御性回退。

路径必须以 `/https://`（或显式开启后 `/http://`）开头；剥掉首个 `/` 得到上游 URL，再校验 scheme、host 白名单。白名单比较两侧都做归一化（去端口、方括号、尾点、大小写），所以 `api.example.com:443`、`[::1]`、`API.Example.COM.` 都能匹配。

### 2. 两阶段处理

| 路径 | 阶段一 | 阶段二 |
| :--- | :--- | :--- |
| 以 `/chat/completions` 结尾 | 请求体读入内存 → 429 重试循环 | 流式透传 |
| 其他 | 直接流式转发 `r.Body`（不缓冲、不重试） | 流式透传 |

chat 路径请求体上限 `maxBodyBytes`（默认 256KB），超出返回 413。重试需要把请求体留在内存，**内存放大 ≈ 并发数 × (`maxBodyBytes` + 64KB)**。透传路径显式设置 `req.ContentLength = r.ContentLength`，避免请求体退化成 chunked。

### 3. 429 分类器

从 429 响应体提取 `error.type` / `error.code` / `error.message` / 顶层 `message` / `detail` / `code` 拼成 haystack（JSON 解析失败则用整个 body），大小写不敏感子串匹配。**禁止匹配裸数字 `429`**。

严格优先级：

```text
quotaExhaustedPatterns（明确配额耗尽）
  → rateLimitPatterns（明确瞬时限流）
  → "insufficient_quota"（模糊配额，语义信号）
  → Retry-After（时序信号）
  → 未分类
```

语义信号优先于时序信号：所以 `insufficient_quota` + `Retry-After` 判为**模糊配额**（走 `ambiguousRamp`），而不是瞬时限流。明确配额耗尽立即透传上游 429，不重试。

### 4. 退避

```text
retryAfterCapped = min(parseRetryAfter(Retry-After), 所选 ramp 的 max(delaysMs) × retryAfterCapRatio)
rampDelay        = computeRampDelay(ramp, rampIndex)
delay            = max(retryAfterCapped, rampDelay)      # Retry-After 是下限
rampIndex++                                             # 恒递增
```

- 正常 ramp 封顶 `10000 × 4 = 40s`；模糊 ramp 封顶 `30000 × 4 = 120s`。
- 缓坡在**单个 ramp 周期内递增、到顶后按取模回绕循环**（非全局单调）。
- 两种典型行为：
  - 持续 `Retry-After: 60s`（normalRamp）→ `min(60s,40s)=40s`，缓坡恒 ≤10s，`delay` **恒为 40s**，表现为「被 Retry-After 压住」。
  - 持续 `Retry-After: 1` → `delay` 为 `2,2,3,3,3,4.5,4.5,6.75,10,2,...`，锯齿生效。
- 退出条件：客户端断开（静默返回、不写响应）、拿到非 429 响应、明确配额耗尽；网络错误另走 502。

### 5. 阶段二透传

删除逐跳头后 `copyHeader` + `WriteHeader`，随后用 32KB buffer 循环读写并 `Flusher.Flush()`，保证 SSE 逐块到达。

**逐跳头**（请求侧 + 响应侧都删，且先用 `Values` 展开 `Connection` / `Proxy-Connection` 的 token 再删固定项）：

```text
Connection, Keep-Alive, Proxy-Authenticate, Proxy-Authorization,
Te, Trailer, Transfer-Encoding, Upgrade, Proxy-Connection
```

**端到端头必须原样保留**：`Content-Encoding`、`Content-Type`、`Retry-After`。上游 gzip 压缩的响应要靠 `Content-Encoding` 才能被客户端正确解压。`Content-Length` 在响应侧不删，只在配额耗尽合成响应时按截断后长度 `Set` **覆盖**（覆盖 ≠ 删除）。

---

## 配置参考

`config.json`（纯 JSON，**不支持注释**）：

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

| 字段 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `listen` | string | `127.0.0.1:18888` | 只允许 `127.0.0.1` / `::1` / `localhost` |
| `allowHttp` | bool | `false` | 是否允许 `http://` 上游；默认关闭以防 Key 明文出网 |
| `allowedUpstreamHosts` | []string | `[]` | 上游 host 白名单；为空允许所有但启动打风险提示 |
| `maxBodyBytes` | int | `262144` | chat 请求体上限（256KB）。带图片 / base64 / 超长上下文需调大；透传路径不受此限制 |
| `logLevel` | string | `info` | `debug` / `info` / `warn` / `error` |
| `normalRamp.delaysMs` | []int | 见示例 | 瞬时限流 / 未分类退避序列，循环使用 |
| `normalRamp.jitterRatio` | float | `0.25` | 抖动比例，范围 `[0,1]` |
| `ambiguousRamp.delaysMs` | []int | 见示例 | 模糊配额退避序列，循环使用 |
| `ambiguousRamp.jitterRatio` | float | `0.25` | 同上 |
| `retryAfterCapRatio` | int | `4` | `Retry-After` 封顶比例，封顶 = 所选 ramp 的 `max(delaysMs) × 该值` |
| `quotaExhaustedPatterns` | []string | 见示例 | 明确配额耗尽子串（大小写不敏感） |
| `rateLimitPatterns` | []string | 见示例 | 明确瞬时限流子串（大小写不敏感） |

**加载顺序**：默认值 → `config.json` → 环境变量 `LISTEN` / `LOG_LEVEL`。

**启动校验**（任一失败即退出）：配置文件存在且可解析；`listen` 为回环地址；`maxBodyBytes >= 1024`；`retryAfterCapRatio >= 1`；两个 ramp 的 `delaysMs` 非空且全部 `> 0`、`jitterRatio ∈ [0,1]`。

`max429BodyBytes` 是编译期常量 `64 * 1024`，**不是配置项**：429 响应体读取上限与请求体上限解耦，分类只需 body 前缀做子串匹配。

---

## 错误响应

统一 `{"error":"..."}`，`Content-Type: application/json; charset=utf-8`。

| 场景 | 状态码 |
| :--- | :--- |
| 缺少上游 URL 前缀 / 解析失败 / scheme 非法 / 读请求体失败 | 400 |
| `http://` 上游且 `allowHttp: false` → `http upstream not allowed; set allowHttp: true to enable` | 400 |
| host 不在白名单 | 403 |
| chat 请求体超过 `maxBodyBytes` | 413 |
| 透传路径上游失败 / chat 网络错误 / `http.NewRequest` 失败（**均不重试**） | 502 |
| 客户端断开（ctx 取消） | 不写响应，静默返回 |
| 明确配额耗尽 | 透传上游 429（前 64KB 截断副本，保留 `Retry-After`） |

---

## 日志

`[INFO]` 级别输出 `req`（8 位 hex）、`attempt`、`upstream`、`status`、`class`、`retry_after`、`sleep_ms`、`final`（`ok` / `passthrough` / `502`）。`final=502` 表示网络错误直接退出；流中断用 `logger.Warn` 记日志后结束。

---

## 测试

```bash
go test ./... -v
```

`main_test.go` 覆盖：URL 提取与白名单归一化、四类分类器优先级、退避与 `Retry-After` 解析、逐跳头展开、合成响应 `Content-Length` 覆盖、错误响应格式、配置校验与环境变量覆盖，以及端到端的 429 重试→200、配额耗尽透传、网络错误 502 不重试、非 chat 透传不重试且 `Content-Length` 不退化、400/403/413、`//` 前缀保留回归。

> 本地 mock 用 `httptest.NewServer` 时需 `allowHttp: true`；`httptest.NewTLSServer` 可配合默认 `allowHttp: false`。
> `-race` 需要 cgo + gcc，本机无 gcc 时跳过。

---

## 已知限制

- 只覆盖 `/chat/completions`；不覆盖新版 `/responses` API，也不匹配带尾部斜杠的 `/chat/completions/`。chat 路径固定 `POST`。
- 429 配额耗尽响应体固定截断到前 64KB，**非字节级透传**；`Content-Length` 按截断后长度重写。
- 压缩 429：网关原样转发客户端的 `Accept-Encoding`，Go Transport 此时不会自动解压；若上游返回 gzip 压缩的 429，分类器拿到的是压缩字节，会退化为 `ClassUnclassified`（走 `normalRamp`）。这是可接受降级——429 响应通常很小、很多上游不压缩；需要覆盖时可临时关掉客户端的 `Accept-Encoding`。**不要删除响应头的 `Content-Encoding`**。
- 429 响应体截断时是提前 `Close`（未读到 EOF），该连接不会归还共享连接池；错误响应场景可接受。
- 网络错误不重试：上游建连后 60s 不发响应头会触发 `ResponseHeaderTimeout` → 502（该超时只覆盖到响应头，不会掐断 SSE 长流）。
- 空 `allowedUpstreamHosts` 默认允许任意上游：回环监听下本机网页可借网关盲打内网。共享机器请务必配置白名单。

---

## 不做什么

Key 池 / 轮换 / 降级、主动限流、配置热重载、Web UI、完整 SSE 缓冲、请求体或响应体 JSON 修改、API Key 管理（无环境变量注入、无兜底）、非 chat 端点重试、网络错误重试、`maxRetries`。

---

## 代码结构（`main.go`）

```text
1.  imports（crand "crypto/rand" + mrand "math/rand" 别名；min/max 用 Go 1.21+ 内建）
2.  常量：hopByHopHeaders、max429BodyBytes、错误消息、gatewayError
3.  类型：Config, RampConfig, Classification, Logger, LogLevel
4.  包级 upstreamClient = newHTTPClient()（ResponseHeaderTimeout 60s、CheckRedirect）
5.  main / loadConfig / validateConfig / errCh + select 优雅关闭
6.  handle / extractUpstreamURL / normalizeHost / handleChat / handlePassthrough
7.  forwardWithRetry / buildSyntheticResponse
8.  classify / extractTextForMatching / readLimited
9.  computeRampDelay / parseRetryAfter / sleepWithContext / rampFor
10. streamResponse / copyHeader / delHopByHop / writeJSONError
11. newRequestID / maxInt
```
