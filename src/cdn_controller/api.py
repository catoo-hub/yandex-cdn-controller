from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

from .controller import Controller
from .models import AppConfig
from .settings import Settings


class ActionRequest(BaseModel):
    actor: str = "api"
    reason: str = "manual"


class ImportRequest(BaseModel):
    resource_id: str
    fqdn: str
    bytes_sent: float = 0
    actor: str = "api"


def create_app(controller: Controller, config: AppConfig, settings: Settings) -> FastAPI:
    scheduler_task: asyncio.Task | None = None

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        nonlocal scheduler_task
        await controller.initialize()
        scheduler_task = asyncio.create_task(controller.run(), name="reconciler")
        yield
        controller.running = False
        if scheduler_task:
            scheduler_task.cancel()
            await asyncio.gather(scheduler_task, return_exceptions=True)
        await controller.close()

    app = FastAPI(title="Yandex CDN Controller", version="0.1.0", lifespan=lifespan)

    async def authorize(authorization: str = Header(default="")) -> None:
        if not settings.controller_token or authorization != f"Bearer {settings.controller_token}":
            raise HTTPException(401, "invalid controller token")

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "time": time.time()}

    @app.get("/readyz")
    async def readyz():
        heartbeat_age = time.time() - controller.scheduler_heartbeat if controller.scheduler_heartbeat else None
        ready = controller.running and heartbeat_age is not None \
            and heartbeat_age < config.poll_interval_seconds * 3
        return JSONResponse({"status": "ready" if ready else "not-ready", "heartbeat_age": heartbeat_age},
                            status_code=200 if ready else 503)

    @app.get("/metrics")
    async def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/v1/status", dependencies=[Depends(authorize)])
    async def status():
        return await controller.status()

    @app.get("/api/v1/events", dependencies=[Depends(authorize)])
    async def events(target: str | None = None, limit: int = 50):
        return await controller.db.events(target, min(limit, 200))

    @app.post("/api/v1/targets/{target_id}/reconcile", dependencies=[Depends(authorize)])
    async def reconcile(target_id: str, body: ActionRequest):
        return await controller.reconcile(target_id, body.actor)

    @app.post("/api/v1/targets/{target_id}/prepare", dependencies=[Depends(authorize)])
    async def prepare(target_id: str, body: ActionRequest):
        return (await controller.prepare(target_id, body.actor)).model_dump(mode="json")

    @app.post("/api/v1/targets/{target_id}/rotate", dependencies=[Depends(authorize)])
    async def rotate(target_id: str, body: ActionRequest):
        return await controller.rotate(target_id, body.actor)

    @app.post("/api/v1/targets/{target_id}/recreate", dependencies=[Depends(authorize)])
    async def recreate(target_id: str, body: ActionRequest):
        return (await controller.recreate_in_place(target_id, body.actor)).model_dump(mode="json")

    @app.post("/api/v1/targets/{target_id}/rollback", dependencies=[Depends(authorize)])
    async def rollback(target_id: str, body: ActionRequest):
        return await controller.rollback(target_id, body.actor)

    @app.post("/api/v1/targets/{target_id}/pause", dependencies=[Depends(authorize)])
    async def pause(target_id: str, body: ActionRequest):
        config.target(target_id)
        await controller.db.set_paused(target_id, True)
        return {"target": target_id, "paused": True}

    @app.post("/api/v1/targets/{target_id}/resume", dependencies=[Depends(authorize)])
    async def resume(target_id: str, body: ActionRequest):
        config.target(target_id)
        await controller.db.set_paused(target_id, False)
        return {"target": target_id, "paused": False}

    @app.post("/api/v1/targets/{target_id}/import", dependencies=[Depends(authorize)])
    async def import_existing(target_id: str, body: ImportRequest):
        config.target(target_id)
        generation = await controller.db.import_active(target_id, body.resource_id, body.fqdn, body.bytes_sent)
        return generation.model_dump(mode="json")

    @app.get("/api/v1/targets/{target_id}/cleanup-preview", dependencies=[Depends(authorize)])
    async def cleanup_preview(target_id: str):
        config.target(target_id)
        candidates = [g.model_dump(mode="json") for g in await controller.db.generations(target_id)
                      if g.state.value == "RETIRED"]
        return {"target": target_id, "candidates": candidates, "automatic_delete": False}

    @app.exception_handler(KeyError)
    async def key_error(_: Request, exc: KeyError):
        return JSONResponse({"detail": f"not found: {exc}"}, status_code=404)

    @app.exception_handler(RuntimeError)
    async def runtime_error(_: Request, exc: RuntimeError):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    return app
