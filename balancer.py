import asyncio
import time
from aiohttp import web, ClientSession

# --- Config: backends and options ---
BACKENDS = [
    {"name": "s1", "url": "http://127.0.0.1:8001", "healthy": True, "weight": 1, "active": 0, "ok": 0, "fail": 0},
    {"name": "s2", "url": "http://127.0.0.1:8002", "healthy": True, "weight": 1, "active": 0, "ok": 0, "fail": 0},
]
HEALTH_PATH = "/health"
HEALTH_INTERVAL = 2.0           # seconds
REQUEST_TIMEOUT = 10            # seconds
STICKY_COOKIE = "LB_NODE"       # set to None to disable stickiness
STICKY_TTL_SECONDS = 600        # cookie lifetime
ROUND_ROBIN_INDEX = 0

# hop-by-hop headers must not be forwarded by proxies
HOP_BY_HOP = {
    "connection","keep-alive","proxy-authenticate","proxy-authorization",
    "te","trailers","transfer-encoding","upgrade"
}

# ---------------- Health checks loop ----------------
async def health_loop(app: web.Application):
    while True:
        async with ClientSession() as sess:
            tasks = [check_backend(sess, b) for b in BACKENDS]
            await asyncio.gather(*tasks)
        await asyncio.sleep(HEALTH_INTERVAL)

async def check_backend(session: ClientSession, backend: dict):
    try:
        async with session.get(backend["url"] + HEALTH_PATH, timeout=REQUEST_TIMEOUT) as resp:
            backend["healthy"] = (resp.status == 200)
    except Exception:
        backend["healthy"] = False

# ---------------- Picking a backend ----------------
def pick_backend_rr():
    """Simple round-robin among healthy backends."""
    global ROUND_ROBIN_INDEX
    healthy = [b for b in BACKENDS if b["healthy"]]
    if not healthy:
        return None
    b = healthy[ROUND_ROBIN_INDEX % len(healthy)]
    ROUND_ROBIN_INDEX += 1
    return b

def pick_backend_sticky(cookie_val: str | None):
    """If cookie matches a healthy backend, use it; else fall back to RR."""
    healthy = [b for b in BACKENDS if b["healthy"]]
    if not healthy:
        return None
    if cookie_val:
        for b in healthy:
            if b["name"] == cookie_val:
                return b
    return pick_backend_rr()

# ---------------- Proxy handler ----------------
async def proxy(request: web.Request):
    start = time.time()

    # choose backend
    cookie_choice = request.cookies.get(STICKY_COOKIE) if STICKY_COOKIE else None
    backend = pick_backend_sticky(cookie_choice) if STICKY_COOKIE else pick_backend_rr()
    if backend is None:
        return web.json_response({"error": "no healthy backends"}, status=503)

    backend["active"] += 1
    try:
        target_url = backend["url"] + request.rel_url.path_qs

        # forward headers: strip hop-by-hop; set X-Forwarded-For/Proto
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP}
        peer = request.transport.get_extra_info('peername')
        client_ip = peer[0] if peer else "unknown"
        xff = headers.get("X-Forwarded-For")
        headers["X-Forwarded-For"] = f"{xff}, {client_ip}" if xff else client_ip
        headers["X-Forwarded-Proto"] = request.scheme

        body = await request.read()

        async with ClientSession() as sess:
            async with sess.request(request.method, target_url, headers=headers, data=body, timeout=REQUEST_TIMEOUT) as resp:
                resp_headers = [(k, v) for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP]
                payload = await resp.read()
                duration_ms = int((time.time() - start) * 1000)

                backend["ok"] += 1
                out = web.Response(status=resp.status, headers=resp_headers, body=payload)

                # set sticky cookie if needed
                if STICKY_COOKIE and (cookie_choice != backend["name"]):
                    out.set_cookie(STICKY_COOKIE, backend["name"], max_age=STICKY_TTL_SECONDS, path="/", httponly=False)

                out.headers["X-LB-Backend"] = backend["name"]
                out.headers["X-LB-Duration-ms"] = str(duration_ms)
                return out
    except Exception:
        backend["fail"] += 1
        return web.json_response({"error": "upstream failure"}, status=502)
    finally:
        backend["active"] -= 1

# ---------------- Aux endpoints on LB ----------------
async def metrics(request: web.Request):
    return web.json_response({
        "backends": [
            {k: v for k, v in b.items() if k in ("name","url","healthy","active","ok","fail","weight")}
            for b in BACKENDS
        ]
    })

async def health(request: web.Request):
    healthy = any(b["healthy"] for b in BACKENDS)
    return web.json_response({"status": "ok" if healthy else "degraded"})

def create_app():
    app = web.Application()
    app.router.add_route("*", "/metrics", metrics)
    app.router.add_route("*", "/health", health)
    app.router.add_route("*", "/{tail:.*}", proxy)  # catch-all proxy
    app.on_startup.append(lambda app: asyncio.create_task(health_loop(app)))
    return app

if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=8000)
