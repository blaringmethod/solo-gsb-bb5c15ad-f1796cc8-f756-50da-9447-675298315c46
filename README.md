# 一次性内容投递箱 API（One-Time Dropbox）

发送者创建一个**带过期时间**的文本或小文件投递，系统返回随机取件令牌与撤回密钥；
收件人凭令牌取件，内容**只能成功读取一次**——成功取件后立即失效。

技术栈：**FastAPI + PostgreSQL + Redis**，容器化一键启动。

- 文本投递默认上限 64 KiB，文件投递默认上限 1 MiB（可用环境变量调整）
- 投递默认 24 小时过期，TTL 上限 7 天
- 并发取件**只有一个请求**能拿到内容，其余得到明确的终态结果
- 撤回、过期、已消费、不存在四类失败各自返回独立的 `reason` 码
- 每个投递附带最小审计记录（创建 / 消费 / 撤回 / 过期 / 拒绝取件）

---

## 一条命令启动

```bash
docker compose up --build
```

启动后端口：

| 服务        | 地址                       | 说明                         |
|-------------|----------------------------|------------------------------|
| **API**     | http://localhost:8000      | 业务接口                     |
| Swagger 文档 | http://localhost:8000/docs | 交互式 API 文档              |
| 健康检查    | http://localhost:8000/health | 检查 PostgreSQL / Redis 连通 |
| PostgreSQL  | localhost:5432             | 库/用户/密码均为 `dropbox`   |
| Redis       | localhost:6379             | 消费串行锁 + 终态缓存        |

停止并清空数据卷：

```bash
docker compose down -v
```

---

## 快速体验

### 1. 创建文本投递

```bash
curl -s -X POST http://localhost:8000/api/deliveries/text \
  -H 'Content-Type: application/json' \
  -d '{"content": "你的一次性验证码：482916", "ttl_seconds": 3600, "sender": "alice"}' | jq
```

返回：

```json
{
  "token": "t1Ab...",
  "kind": "text",
  "pickup_url": "http://localhost:8000/api/deliveries/t1Ab.../pickup",
  "withdraw_url": "http://localhost:8000/api/deliveries/t1Ab.../withdraw",
  "status_url": "http://localhost:8000/api/deliveries/t1Ab.../status",
  "withdraw_secret": "s3Cr...",
  "expires_at": "2026-09-21T12:00:00+00:00",
  "ttl_seconds": 3600,
  "size_bytes": 40
}
```

`token` 是取件令牌，可公开给收件人；`withdraw_secret` 是发送者撤回密钥，请妥善保管。

### 2. 取件（只有第一次成功）

```bash
curl -s -X POST http://localhost:8000/api/deliveries/<TOKEN>/pickup | jq
# 200 -> {"kind": "text", "content": "你的一次性验证码：482916", ...}

curl -s -X POST http://localhost:8000/api/deliveries/<TOKEN>/pickup | jq
# 410 -> {"error": true, "reason": "consumed", "message": "投递已被消费..."}
```

文件投递取件成功时直接返回二进制流（`Content-Disposition: attachment`）。

### 3. 并发取件只有一个赢家

```bash
for i in 1 2 3 4 5; do
  curl -s -o /tmp/out_$i -w '%{http_code}\n' \
    -X POST http://localhost:8000/api/deliveries/<TOKEN>/pickup &
done; wait
# 恰好一个 200，其余为 410（consumed）；与赢家同时抢锁的请求可能得到 409（in_progress，可重试）
```

### 4. 撤回

```bash
curl -s -X POST "http://localhost:8000/api/deliveries/<TOKEN>/withdraw" \
  -H 'X-Withdraw-Secret: <SECRET>' | jq
# 200 -> {"state": "withdrawn", ...}；撤回后取件得到 410 withdrawn
```

### 5. 查询状态与审计

```bash
curl -s "http://localhost:8000/api/deliveries/<TOKEN>/status" \
  -H 'X-Withdraw-Secret: <SECRET>' | jq

curl -s "http://localhost:8000/api/deliveries/<TOKEN>/audit?limit=20" \
  -H 'X-Withdraw-Secret: <SECRET>' | jq
```

### 6. 文件投递

```bash
curl -s -X POST http://localhost:8000/api/deliveries/files \
  -F 'file=@./secret.txt' \
  -F 'ttl_seconds=600' \
  -F 'sender=bob' | jq

curl -s -X POST http://localhost:8000/api/deliveries/<TOKEN>/pickup -OJ
```

---

## 接口一览

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| POST | `/api/deliveries/text` | 创建文本投递（JSON） | 无 |
| POST | `/api/deliveries/files` | 创建文件投递（multipart） | 无 |
| POST | `/api/deliveries/{token}/pickup` | 一次性取件（成功即销毁） | 无（令牌即凭证） |
| POST | `/api/deliveries/{token}/withdraw` | 撤回投递 | `X-Withdraw-Secret` 或 `?withdraw_secret=` |
| GET  | `/api/deliveries/{token}/status` | 查询状态 | 同上 |
| GET  | `/api/deliveries/{token}/audit` | 查询审计记录 | 同上 |
| GET  | `/health` | 健康检查 | 无 |

### 结果码

| HTTP | reason | 含义 |
|------|--------|------|
| 200 | — | 取件成功（唯一一次能拿到内容的响应） |
| 201 | — | 创建/撤回成功 |
| 404 | `not_found` | 投递不存在或令牌无效 |
| 409 | `in_progress` | 已有取件请求正在处理（带 `Retry-After: 1`，可重试） |
| 410 | `consumed` | 已被成功取件消费 |
| 410 | `withdrawn` | 已被发送者撤回 |
| 410 | `expired` | 已超过过期时间 |
| 401/403 | `wrong_secret` | 缺少 / 错误的撤回密钥 |
| 413 | `payload_too_large` | 内容超过大小上限 |
| 422 | `invalid_ttl` | TTL 非法 |

错误响应统一为：

```json
{"error": true, "reason": "expired", "message": "...", "request_id": "..."}
```

---

## 原子消费是如何保证的

取件路径有两道相互独立的保险，PostgreSQL 是权威数据源：

1. **Redis 串行锁（快速路径）**
   进入取件临界区前执行 `SET pickup:lock:<token> <request-id> NX EX 15`。
   拿不到锁的并发请求立即返回 `409 in_progress`，不会打数据库。
   锁带 15 秒 TTL（持锁进程崩溃时自动释放），释放用 Lua 脚本校验持有者，避免误删。

2. **PostgreSQL 单条条件 UPDATE（正确性底线）**
   临界区内执行一条原子语句：

   ```sql
   UPDATE deliveries
   SET consumed_at = now()
   WHERE token = $1
     AND consumed_at IS NULL
     AND withdrawn_at IS NULL
     AND expired_at IS NULL
     AND now() <= expires_at
   RETURNING *;
   ```

   行级更新在数据库内原子串行化：**即使 Redis 被清空、多个请求同时穿透，
   也只有一个 UPDATE 命中并返回内容**，其余 0 行命中，重读后明确归类为
   `consumed` / `withdrawn` / `expired`。内容不在 Redis 中存储，Redis 失效不影响安全性。

3. **终态缓存**
   `consumed` / `withdrawn` / `expired` / `not_found` 会写入 Redis 短期缓存（默认 1 小时），
   后续取件直接短路返回明确结果，减轻数据库压力；缓存只做加速，不参与安全判定。

4. **过期**
   取件本身用 `now() <= expires_at` 兜底；后台 reaper（默认每 30 秒）
   用互斥条件 `UPDATE ... SET expired_at = now()` 把到期投递原子标记为 expired
   （`FOR UPDATE SKIP LOCKED`），并写审计、刷新终态缓存。

---

## 数据模型

```
deliveries
  token            取件令牌（UNIQUE，secrets.token_urlsafe(32)）
  withdraw_secret  撤回密钥（仅创建响应中返回一次）
  kind             text | file
  content          BYTEA（PostgreSQL 存储，不落 Redis）
  filename / content_type
  created_at / expires_at
  consumed_at / withdrawn_at / expired_at   三个互斥终态时间戳
  sender

audit_events
  event_time, token, action, actor, detail(JSONB)
  action ∈ created | consumed | pickup_denied | withdrawn
         | withdraw_denied | withdraw_idempotent | expired
```

---

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DATABASE_URL` | `postgresql://dropbox:dropbox@db:5432/dropbox` | PostgreSQL DSN |
| `REDIS_URL` | `redis://redis:6379/0` | Redis 地址 |
| `MAX_TEXT_BYTES` | `65536` | 文本投递大小上限 |
| `MAX_FILE_BYTES` | `1048576` | 文件投递大小上限（1 MiB） |
| `DEFAULT_TTL_SECONDS` | `86400` | 默认 TTL |
| `MAX_TTL_SECONDS` | `604800` | TTL 上限 |
| `REAPER_INTERVAL_SECONDS` | `30` | 后台过期扫描间隔 |
| `LOCK_TTL_SECONDS` | `15` | 取件锁兜底 TTL |
| `TERMINAL_CACHE_TTL_SECONDS` | `3600` | 终态结果缓存 TTL |

---

## 本地开发（不用 Docker）

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 自行准备 PostgreSQL 与 Redis，然后：
export DATABASE_URL=postgresql://dropbox:dropbox@localhost:5432/dropbox
export REDIS_URL=redis://localhost:6379/0
uvicorn app.main:app --reload --port 8000
```

表结构在应用启动时通过 `CREATE TABLE IF NOT EXISTS` 幂等创建，无需手动迁移。

---

## 说明与边界

- 本项目演示“一次性读取”原语，**令牌/密钥以明文存储**于数据库；生产环境应只存哈希并改用 HTTPS。
- 投递内容存储在 PostgreSQL（BYTEA），定位为“文本或小文件”，大文件请改用对象存储 + 单次签名 URL。
- `in_progress` 是正常的并发背压结果，客户端应按 `Retry-After` 重试；重试点必然得到
  `consumed`（赢家已消费）或 200（持锁者崩溃且锁过期，自己成为新赢家）。
