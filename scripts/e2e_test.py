"""端到端验证：嵌入式 PostgreSQL + fakeredis + FastAPI ASGI。

覆盖：
- 文本/文件创建、取件一次成功、二次取件 410 consumed
- Redis 锁路径下并发取件只有一个 200（409 自动重试后仍只有一个赢家）
- Redis 被击穿（锁恒成功 + 清空终态缓存）时，仅靠 PG 条件 UPDATE
  并发取件仍然只有一个 200 —— 验证原子消费的数据库底线
- 撤回、错误密钥、过期（reaper）、状态查询、审计记录
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile

import fakeredis.aioredis
import httpx
import pgserver

# ---- 准备嵌入式 PostgreSQL --------------------------------------------------
_PG_DIR = tempfile.mkdtemp(prefix="pgtest_")
_srv = pgserver.get_server(_PG_DIR)
_socket_host = _srv.get_uri().split("host=")[-1].rstrip("/")
os.environ["DATABASE_URL"] = f"postgresql://postgres@/postgres?host={_socket_host}"
os.environ["REDIS_URL"] = "redis://localhost:6379/0"
os.environ["REAPER_INTERVAL_SECONDS"] = "3600"  # 测试中手动驱动过期标记
os.environ["DEFAULT_TTL_SECONDS"] = "3600"

from app import redis_store  # noqa: E402
from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


async def main() -> int:
    # ---- 用 fakeredis 替换真实 Redis（必须在 lifespan 之前完成）----------
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    redis_store.client = fake
    # lifespan 里的 init_redis() 会连接真实 Redis；替换为 no-op
    redis_store.init_redis = lambda: asyncio.sleep(0)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as ac:
        # 手动跑 lifespan（init_db 等）
        async with app.router.lifespan_context(app):
            print("== 生命周期/健康检查 ==")
            r = await ac.get("/health")
            check("health 200", r.status_code == 200, r.text)
            check("health db/redis ok",
                  r.json()["database"] == "ok" and r.json()["redis"] == "ok", r.text)

            print("== 文本创建 + 一次性取件 ==")
            r = await ac.post("/api/deliveries/text", json={
                "content": "秘密：482916", "ttl_seconds": 3600, "sender": "alice"})
            check("create text 201", r.status_code == 201, r.text)
            d = r.json()
            token, secret = d["token"], d["withdraw_secret"]
            check("返回 token/secret", bool(token) and bool(secret))
            check("size_bytes 正确", d["size_bytes"] == len("秘密：482916".encode()))

            r = await ac.post(f"/api/deliveries/{token}/pickup")
            check("首次取件 200", r.status_code == 200, r.text)
            check("内容一致", r.json().get("content") == "秘密：482916", r.text)
            check("X-Delivery-Kind=text",
                  r.headers.get("x-delivery-kind") == "text")

            r = await ac.post(f"/api/deliveries/{token}/pickup")
            check("二次取件 410", r.status_code == 410, r.text)
            check("reason=consumed", r.json().get("reason") == "consumed", r.text)

            r = await ac.post("/api/deliveries/{}/pickup".format("x" * 43))
            check("未知令牌 404", r.status_code == 404, r.text)
            check("reason=not_found", r.json().get("reason") == "not_found")

            print("== 文件创建 + 取件 ==")
            blob = bytes(range(256)) * 4
            files = {"file": ("secret blob.bin", blob, "application/octet-stream")}
            data = {"ttl_seconds": "120", "sender": "bob"}
            r = await ac.post("/api/deliveries/files", files=files, data=data)
            check("create file 201", r.status_code == 201, r.text)
            ftoken = r.json()["token"]
            check("kind=file", r.json()["kind"] == "file")
            r = await ac.post(f"/api/deliveries/{ftoken}/pickup")
            check("文件取件 200", r.status_code == 200, r.text)
            check("文件字节一致", r.content == blob, f"len={len(r.content)}")
            cd = r.headers.get("content-disposition", "")
            check("附件下载头", "attachment" in cd and "secret%20blob.bin" in cd, cd)
            r = await ac.post(f"/api/deliveries/{ftoken}/pickup")
            check("文件二次取件 410 consumed",
                  r.status_code == 410 and r.json()["reason"] == "consumed", r.text)

            print("== 大文件 413 ==")
            big = b"a" * (settings.max_file_bytes + 1)
            r = await ac.post("/api/deliveries/files",
                              files={"file": ("big.bin", big)},
                              data={"ttl_seconds": "60"})
            check("超限 413", r.status_code == 413 and
                  r.json()["reason"] == "payload_too_large", r.text)

            print("== 并发取件（Redis 锁路径，409 自动重试）==")
            r = await ac.post("/api/deliveries/text",
                              json={"content": "并发内容", "ttl_seconds": 600})
            ctoken = r.json()["token"]

            async def pickup_with_retry(t: str, n: int) -> httpx.Response:
                last = None
                for _ in range(10):
                    last = await ac.post(f"/api/deliveries/{t}/pickup")
                    if last.status_code != 409:
                        return last
                    await asyncio.sleep(0.05)
                return last  # type: ignore[return-value]

            results = await asyncio.gather(*[
                pickup_with_retry(ctoken, i) for i in range(12)
            ])
            codes = sorted(r.status_code for r in results)
            winners = [r for r in results if r.status_code == 200]
            check("恰好一个 200", len(winners) == 1, f"codes={codes}")
            check("赢家内容正确",
                  winners[0].json().get("content") == "并发内容")
            check("其余全部 410",
                  all(r.status_code == 410 and r.json()["reason"] == "consumed"
                      for r in results if r.status_code != 200),
                  f"{[(r.status_code, r.json().get('reason')) for r in results]}")

            print("== 击穿 Redis：仅靠 PostgreSQL 条件 UPDATE 保证唯一赢家 ==")
            r = await ac.post("/api/deliveries/text",
                              json={"content": "DB 底线内容", "ttl_seconds": 600})
            btoken = r.json()["token"]

            # 锁永远可获取 + 每次调用前清空终态缓存 => 全部请求直打数据库
            orig_acquire = redis_store.acquire_pickup_lock
            orig_get = redis_store.get_terminal

            async def acquire_always(token: str, request_id: str) -> bool:
                return True

            async def empty_terminal(token: str):
                return None

            redis_store.acquire_pickup_lock = acquire_always
            redis_store.get_terminal = empty_terminal
            try:
                # 直接调用服务层，绕过 Redis 缓存短路与锁
                from app import services

                async def raw_pickup(i: int):
                    return await services.pickup(
                        btoken, f"breaker-{i}")

                outcomes = await asyncio.gather(*[raw_pickup(i) for i in range(12)])
                wins = [o for o in outcomes
                        if o.reason == "consumed" and o.row is not None]
                check("击穿后仍恰好一个赢家", len(wins) == 1,
                      f"reasons={sorted(o.reason for o in outcomes)}")
                check("输家全部 consumed",
                      all(o.reason == "consumed" and o.row is None
                          for o in outcomes if o not in wins),
                      f"{sorted(o.reason for o in outcomes)}")
                check("赢家内容正确",
                      wins[0].row["content"].decode() == "DB 底线内容")
            finally:
                redis_store.acquire_pickup_lock = orig_acquire
                redis_store.get_terminal = orig_get

            # 直接再打一次 API，应读到终态（数据库行已是 consumed）
            r = await ac.post(f"/api/deliveries/{btoken}/pickup")
            check("击穿测试后 API 取件为 410",
                  r.status_code == 410 and r.json()["reason"] == "consumed", r.text)

            print("== 撤回 ==")
            r = await ac.post("/api/deliveries/text",
                              json={"content": "待撤回", "ttl_seconds": 600})
            w = r.json()
            wtoken, wsecret = w["token"], w["withdraw_secret"]

            r = await ac.post(f"/api/deliveries/{wtoken}/withdraw")
            check("无密钥 401", r.status_code == 401 and
                  r.json()["reason"] == "wrong_secret", r.text)
            r = await ac.post(f"/api/deliveries/{wtoken}/withdraw",
                              headers={"X-Withdraw-Secret": "bad"})
            check("错密钥 403", r.status_code == 403 and
                  r.json()["reason"] == "wrong_secret", r.text)
            r = await ac.post(f"/api/deliveries/{wtoken}/withdraw",
                              headers={"X-Withdraw-Secret": wsecret})
            check("撤回 200", r.status_code == 200 and
                  r.json()["state"] == "withdrawn", r.text)
            r = await ac.post(f"/api/deliveries/{wtoken}/withdraw",
                              params={"withdraw_secret": wsecret})
            check("重复撤回幂等 200",
                  r.status_code == 200 and r.json()["state"] == "withdrawn", r.text)
            r = await ac.post(f"/api/deliveries/{wtoken}/pickup")
            check("撤回后取件 410 withdrawn",
                  r.status_code == 410 and r.json()["reason"] == "withdrawn", r.text)
            r = await ac.get(f"/api/deliveries/{wtoken}/status",
                             headers={"X-Withdraw-Secret": wsecret})
            check("状态 withdrawn",
                  r.status_code == 200 and r.json()["state"] == "withdrawn", r.text)

            print("== 撤回与取件竞争：只能一方成功 ==")
            r = await ac.post("/api/deliveries/text",
                              json={"content": "竞争中", "ttl_seconds": 600})
            rt = r.json()
            rtoken, rsecret = rt["token"], rt["withdraw_secret"]
            await fake.flushall()  # 清掉终态缓存，锁键也清空

            async def race_pickup():
                return await ac.post(f"/api/deliveries/{rtoken}/pickup")

            async def race_withdraw():
                return await ac.post(
                    f"/api/deliveries/{rtoken}/withdraw",
                    headers={"X-Withdraw-Secret": rsecret})

            rp, rw = await asyncio.gather(race_pickup(), race_withdraw())
            pickup_won = (rp.status_code == 200 and rw.status_code == 409)
            withdraw_won = (rw.status_code == 200
                            and rp.status_code in (410, 409))
            busy = (rp.status_code == 409 and rw.status_code in (200, 409)) \
                or (rw.status_code == 409 and rp.status_code in (200, 410))
            check("取件/撤回互斥（恰一方成功，或一方收到 409 背压）",
                  pickup_won or withdraw_won or busy,
                  f"pickup={rp.status_code}/{rp.json().get('reason')} "
                  f"withdraw={rw.status_code}/{rw.json().get('reason')}")

            print("== 过期（reaper 标记 + 取件判定）==")
            r = await ac.post("/api/deliveries/text",
                              json={"content": "短命", "ttl_seconds": 1})
            e = r.json()
            etoken = e["token"]
            await asyncio.sleep(1.1)
            # 未跑 reaper 前，取件本身即应判定 expired
            r = await ac.post(f"/api/deliveries/{etoken}/pickup")
            check("到期取件 410 expired",
                  r.status_code == 410 and r.json()["reason"] == "expired", r.text)

            # 再建一个到期投递，由 reaper 标记 expired_at
            r = await ac.post("/api/deliveries/text",
                              json={"content": "短命2", "ttl_seconds": 1})
            e2 = r.json()
            e2token = e2["token"]
            await asyncio.sleep(1.1)
            await fake.flushall()
            from app import services as services_mod
            n = await services_mod.mark_due_expirations()
            check("reaper 至少标记 1 个", n >= 1, f"n={n}")
            r = await ac.get(f"/api/deliveries/{e2token}/status",
                             headers={"X-Withdraw-Secret": e2["withdraw_secret"]})
            check("状态 expired",
                  r.json()["state"] == "expired" and
                  r.json()["expired_at"] is not None, r.text)
            r = await ac.post(f"/api/deliveries/{e2token}/withdraw",
                              headers={"X-Withdraw-Secret": e2["withdraw_secret"]})
            check("过期后撤回 409 expired",
                  r.status_code == 409 and r.json()["reason"] == "expired", r.text)

            print("== 审计记录 ==")
            r = await ac.get(f"/api/deliveries/{token}/audit",
                             headers={"X-Withdraw-Secret": "wrong"})
            check("审计错密钥 403", r.status_code == 403, r.text)

            # 清掉终态缓存后再次取件：走数据库路径，拒绝应落审计
            await fake.flushall()
            r = await ac.post(f"/api/deliveries/{token}/pickup")
            check("清缓存后取件仍 410",
                  r.status_code == 410 and r.json()["reason"] == "consumed", r.text)
            r = await ac.get(f"/api/deliveries/{token}/audit",
                             headers={"X-Withdraw-Secret": secret})
            actions = [e["action"] for e in r.json()]
            check("审计含 created/consumed",
                  "created" in actions and "consumed" in actions, str(actions))
            check("审计含拒绝记录",
                  any(a == "pickup_denied" for a in actions), str(actions))

            r = await ac.get(f"/api/deliveries/{e2token}/audit",
                             headers={"X-Withdraw-Secret": e2["withdraw_secret"]})
            actions = [x["action"] for x in r.json()]
            check("审计含 system expired", "expired" in actions, str(actions))

    print(f"\n结果：{PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
