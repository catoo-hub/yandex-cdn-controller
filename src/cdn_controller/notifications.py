from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass

import httpx

from .clients import webhook_signature
from .db import Database
from .metrics import TELEGRAM_NOTIFICATIONS
from .settings import Settings

log = logging.getLogger(__name__)


@dataclass
class Event:
    target_id: str | None
    severity: str
    kind: str
    message: str
    dedup_key: str | None = None
    actor: str | None = None


class Notifier:
    def __init__(self, db: Database, settings: Settings, dedup_seconds: int):
        self.db = db
        self.settings = settings
        self.dedup_seconds = dedup_seconds

    async def emit(self, event: Event) -> None:
        await self.db.add_event(**event.__dict__)
        key = event.dedup_key or f"{event.target_id}:{event.kind}:{event.message}"
        if not await self.db.claim_notification(key, self.dedup_seconds):
            return
        log.log(getattr(logging, event.severity, logging.INFO), event.message,
                extra={"target": event.target_id, "kind": event.kind})
        await asyncio.gather(self._telegram(event), self._webhook(event), return_exceptions=True)

    async def _telegram(self, event: Event) -> None:
        if not self.settings.telegram_bot_token or not self.settings.chat_ids:
            return
        text = f"[{event.severity}] {event.kind}\n{event.message}"
        if event.target_id:
            text = f"Target: {event.target_id}\n{text}"
        async with httpx.AsyncClient(timeout=10) as client:
            for chat_id in self.settings.chat_ids:
                try:
                    payload = {"chat_id": chat_id, "text": text}
                    thread_id = self.settings.topic_map.get(chat_id)
                    if thread_id is not None:
                        payload["message_thread_id"] = thread_id
                    response = await client.post(
                        f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage",
                        json=payload,
                    )
                    response.raise_for_status()
                    TELEGRAM_NOTIFICATIONS.labels(event.severity, "success").inc()
                except httpx.HTTPError:
                    TELEGRAM_NOTIFICATIONS.labels(event.severity, "error").inc()
                    log.exception("telegram notification failed")

    async def _webhook(self, event: Event) -> None:
        if not self.settings.generic_webhook_url:
            return
        body = json.dumps(event.__dict__, separators=(",", ":")).encode()
        headers = {"Content-Type": "application/json"}
        if self.settings.generic_webhook_secret:
            headers["X-CDN-Signature-SHA256"] = webhook_signature(self.settings.generic_webhook_secret, body)
        async with httpx.AsyncClient(timeout=10) as client:
            for attempt in range(3):
                try:
                    response = await client.post(self.settings.generic_webhook_url, content=body, headers=headers)
                    response.raise_for_status()
                    return
                except httpx.HTTPError:
                    if attempt == 2:
                        log.exception("generic webhook failed")
                    else:
                        await asyncio.sleep(2 ** attempt)

    async def kuma(self, url: str, healthy: bool, message: str, ping_ms: int | None = None) -> None:
        if not url:
            return
        params = {"status": "up" if healthy else "down", "msg": message}
        if ping_ms is not None:
            params["ping"] = str(ping_ms)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
        except httpx.HTTPError:
            log.exception("uptime kuma push failed")
