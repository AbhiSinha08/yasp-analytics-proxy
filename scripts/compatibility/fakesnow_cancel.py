"""Cancellation fix for the pinned local Fakesnow HTTP fixture.

The upstream abort route acknowledges without interrupting execution. This fixture
matches the session and request UUID, calls DuckDB interrupt, and records that the
actual query handler has stopped. It never substitutes query results. Private
Fakesnow access is confined here and pinned by requirements.lock.
"""

from contextlib import contextmanager
import gzip
import importlib
import json

from fakesnow import server as _serve


@contextmanager
def server(*, interrupt=True, chunk_mode=None):
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    module = importlib.import_module("fakesnow.server")
    original = list(module.app.router.routes)
    active = {}
    counts = {"started": 0, "aborted": 0, "stopped": 0, "sessions_opened": 0, "sessions_closed": 0}

    if chunk_mode is not None:
        from fakesnow_chunks import ChunkFixture
        chunks = ChunkFixture(chunk_mode, counts)
    else:
        chunks = None

    async def login(request):
        response = await module.login_request(request)
        body = json.loads(response.body)
        if body.get("success"):
            counts["sessions_opened"] += 1
            # Python Connector requires a master token to send session deletion.
            # Fakesnow accepts the same fixture token for both token kinds.
            body["data"].setdefault("masterToken", body["data"]["token"])
            return JSONResponse(body, status_code=response.status_code)
        return response

    async def session(request):
        token = module.to_token(request)
        existed = token in module.sessions
        response = await module.session(request)
        if existed and token not in module.sessions:
            counts["sessions_closed"] += 1
        return response

    async def query(request):
        token = module.to_token(request)
        request_id = request.query_params.get("requestId")
        body = await request.body()
        if request.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        sql = json.loads(body)["sqlText"]
        long_query = "ROWCOUNT => 100000000" in sql
        if long_query:
            counts["started"] += 1
        active[token] = request_id
        try:
            response = await module.query_request(request)
            return chunks.rewrite(request, response, sql) if chunks else response
        finally:
            active.pop(token, None)
            if long_query:
                counts["stopped"] += 1

    async def abort(request):
        token = module.to_token(request)
        body = await request.body()
        if request.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        request_id = json.loads(body).get("requestId")
        if request_id and active.get(token) == request_id:
            if interrupt:
                module.to_conn(token)._duck_conn.interrupt()
            counts["aborted"] += 1
            return JSONResponse({"success": True})
        return JSONResponse({"success": False, "message": "no matching active request"})

    module.app.router.routes = [
        *(chunks.routes() if chunks else []),
        Route("/session/v1/login-request", login, methods=["POST"]),
        Route("/session", session, methods=["POST"]),
        Route("/queries/v1/query-request", query, methods=["POST"]),
        Route("/queries/v1/abort-request", abort, methods=["POST"]),
        *[route for route in original if route.path not in (
            "/session/v1/login-request", "/session", "/queries/v1/query-request", "/queries/v1/abort-request")],
    ]
    try:
        with _serve() as settings:
            yield settings, counts
    finally:
        module.app.router.routes = original
