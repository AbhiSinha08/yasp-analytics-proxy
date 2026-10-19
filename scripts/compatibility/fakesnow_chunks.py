"""Small Snowflake Arrow chunk fixture for fetch cancellation and prefetch gates.

Fakesnow executes the query normally. Its seventeen-row Arrow result is split into
an inline row and sixteen downloadable chunks, retaining the exact schema/values.
"""

import asyncio
import base64
import json
import time
import uuid


class ChunkFixture:
    def __init__(self, mode, counts):
        self.mode = mode
        self.counts = counts
        self.payloads = {}
        counts.update(chunk_requests=0, chunk_active=0, chunk_stopped=0, chunk_cancelled=0)

    def routes(self):
        from starlette.responses import JSONResponse
        from starlette.routing import Route
        return [
            Route("/_yasp_probe/chunks/{key}", self.chunk),
            Route("/_yasp_probe/status", lambda _: JSONResponse(dict(self.counts))),
        ]

    def rewrite(self, request, response, sql):
        if "yasp-fetch-probe" not in sql:
            return response
        import pyarrow
        from starlette.responses import JSONResponse
        body = json.loads(response.body)
        data = body["data"]
        table = pyarrow.ipc.open_stream(base64.b64decode(data["rowsetBase64"])).read_all()
        if table.num_rows != 17:
            raise AssertionError("chunk fixture query must return exactly seventeen rows")

        def ipc(row):
            sink = pyarrow.BufferOutputStream()
            with pyarrow.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table.slice(row, 1))
            return sink.getvalue().to_pybytes()

        data["rowsetBase64"] = base64.b64encode(ipc(0)).decode()
        data["chunks"] = []
        data["chunkHeaders"] = {}
        for row in range(1, 17):
            key = uuid.uuid4().hex
            payload = ipc(row)
            self.payloads[key] = payload
            data["chunks"].append({"url": str(request.base_url).rstrip("/") + "/_yasp_probe/chunks/" + key,
                                   "rowCount": 1, "uncompressedSize": len(payload), "compressedSize": len(payload)})
        return JSONResponse(body)

    async def chunk(self, request):
        from starlette.responses import Response
        self.counts["chunk_requests"] += 1
        self.counts["chunk_active"] += 1
        try:
            if self.mode == "cancel":
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    if await request.is_disconnected():
                        self.counts["chunk_cancelled"] += 1
                        return Response(status_code=499)
                    await asyncio.sleep(0.01)
            return Response(self.payloads[request.path_params["key"]], media_type="application/octet-stream")
        finally:
            self.counts["chunk_active"] -= 1
            self.counts["chunk_stopped"] += 1
