"""Small native Python HTTP adapter for the local Laya decision model."""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import partial
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


# Keep runtime choices in environment variables so this file runs across platforms.
MODEL_ID = os.getenv("LAYA_MODEL", "convaiinnovations/laya-typed-decisions")
DEVICE = os.getenv("LAYA_DEVICE", "auto").lower()
HOST = os.getenv("LAYA_HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8001"))
MAX_CONCURRENT = int(os.getenv("LAYA_MAX_CONCURRENT", "8"))
MAX_TOKEN_BUDGET = int(os.getenv("LAYA_MAX_TOKEN_BUDGET", "2048"))
THREADS = int(os.getenv("LAYA_THREADS", "4"))
LOG_LEVEL = os.getenv("LAYA_LOG_LEVEL", "debug").lower()
REQUIRE_GPU = os.getenv("LAYA_REQUIRE_GPU", "0") == "1"

if DEVICE not in {"auto", "cuda", "mps", "xpu", "cpu"}:
    raise ValueError(f"Unsupported LAYA_DEVICE: {DEVICE}")
if MAX_CONCURRENT < 1 or MAX_TOKEN_BUDGET < 1 or THREADS < 1:
    raise ValueError("Concurrency, token budget, and thread settings must be positive")

logging.basicConfig(level=LOG_LEVEL.upper())
logger = logging.getLogger("laya-server")


class JevRequest(BaseModel):
    state: Any
    questions: dict[str, Any]
    keep_alive: Optional[str] = None
    # max_len is optional; when omitted, Laya uses the checkpoint's default context.
    max_len: Optional[int] = Field(default=None, ge=1, le=MAX_TOKEN_BUDGET)


def load_agent() -> Any:
    import torch
    import laya

    torch.set_num_threads(THREADS)
    logger.info("Loading Laya model %s on %s", MODEL_ID, DEVICE)
    # Let Laya choose an installed accelerator in auto mode, then report what loaded.
    agent = laya.load(MODEL_ID, device=None if DEVICE == "auto" else DEVICE)
    info = runtime_info(agent)
    logger.info("Laya runtime: %s", info)
    if REQUIRE_GPU and agent.device.type == "cpu":
        raise RuntimeError(
            "A GPU was required, but Laya loaded on CPU. Check that the installed "
            "PyTorch build supports the selected device and its driver/runtime is available."
        )
    return agent


def runtime_info(agent: Any) -> dict[str, Any]:
    import torch

    device = agent.device
    device_type = device.type
    gpu_name = None
    if device_type == "cuda":
        backend = "ROCm" if torch.version.hip else "CUDA"
        gpu_name = torch.cuda.get_device_name(device.index or 0)
    elif device_type == "mps":
        backend = "Apple Metal (MPS)"
        gpu_name = "Apple Metal GPU"
    elif device_type == "xpu":
        backend = "Intel XPU"
        gpu_name = torch.xpu.get_device_name(device.index or 0)
    else:
        backend = "CPU"

    return {
        "os": platform.system(),
        "architecture": platform.machine(),
        "device": str(device),
        "backend": backend,
        "gpu": gpu_name,
        "pytorch_version": str(torch.__version__),
        "cuda_available": torch.cuda.is_available(),
        "mps_available": (
            hasattr(torch.backends, "mps") and torch.backends.mps.is_available()
        ),
        "xpu_available": hasattr(torch, "xpu") and torch.xpu.is_available(),
    }


def decision_response(raw: dict[str, Any]) -> dict[str, Any]:
    """Keep the PII yes/no score, confidence, and useful truncation diagnostics."""
    raw_answers = raw.get("answers")
    if not isinstance(raw_answers, dict):
        raise ValueError("Laya returned no answers object")

    answers: dict[str, dict[str, Any]] = {}
    for question_id, answer in raw_answers.items():
        if (
            not isinstance(answer, dict)
            or answer.get("type") != "noul"
            or "noul" not in answer
            or "confidence" not in answer
        ):
            raise ValueError(f"Laya returned an unsupported answer for {question_id}")
        # Keep the PII yes-probability and confidence; omit duplicate/action fields.
        answers[question_id] = {
            "type": "noul",
            "noul": answer["noul"],
            "confidence": answer["confidence"],
        }

    raw_usage = raw.get("usage") or {}
    usage = {
        key: raw_usage[key]
        for key in (
            "input_tokens",
            "state_tokens",
            "state_tokens_dropped",
            "truncated",
            "truncated_questions",
        )
        if key in raw_usage
    }
    return {
        "model": raw.get("model", MODEL_ID),
        "answers": answers,
        "usage": usage,
    }


def create_app(agent: Any) -> FastAPI:
    # Requests can be admitted concurrently, but this shared model runs one inference at a time.
    inference_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="laya-infer")

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        inference_pool.shutdown(wait=True)

    app = FastAPI(title="Laya PII Decision API", lifespan=lifespan)

    @app.get("/health")
    def health_check() -> dict[str, Any]:
        return {"status": "healthy", "model": MODEL_ID, "runtime": runtime_info(agent)}

    @app.post("/v1/systemone")
    async def system_one_decision(payload: JevRequest) -> dict[str, Any]:
        # Laya accepts plain strings as state, but normalize them to its text object shape.
        state = {"text": payload.state} if isinstance(payload.state, str) else payload.state
        predict = partial(agent.predict, state=state, questions=payload.questions)
        if payload.max_len is not None:
            predict = partial(predict, max_len=payload.max_len)

        loop = asyncio.get_running_loop()
        started = time.perf_counter()
        try:
            raw = await loop.run_in_executor(inference_pool, predict)
            response = decision_response(raw)
            # Log performance and token metadata only; never log SQL or sampled values.
            logger.debug(
                "Decision completed latency_ms=%.1f questions=%d input_tokens=%s truncated=%s",
                (time.perf_counter() - started) * 1000,
                len(response["answers"]),
                response["usage"].get("input_tokens"),
                response["usage"].get("truncated"),
            )
            return response
        except Exception as error:
            logger.error("Laya inference failed error_type=%s", type(error).__name__)
            raise HTTPException(status_code=500, detail="Laya inference failed") from None

    return app


def main() -> None:
    agent = load_agent()
    uvicorn.run(
        create_app(agent),
        host=HOST,
        port=PORT,
        workers=1,
        limit_concurrency=MAX_CONCURRENT,
        log_level=LOG_LEVEL,
        access_log=True,
    )


if __name__ == "__main__":
    main()
