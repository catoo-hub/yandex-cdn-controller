from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

from .models import Generation, RotationState


SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS generations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  fqdn TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL,
  resource_id TEXT,
  certificate_id TEXT,
  provider_cname TEXT,
  dns_record_id TEXT,
  validation_record_id TEXT,
  bytes_sent REAL NOT NULL DEFAULT 0,
  last_metric_ts REAL,
  created_at REAL NOT NULL,
  activated_at REAL,
  drain_after REAL,
  metadata TEXT NOT NULL DEFAULT '{}',
  UNIQUE(target_id, sequence)
);
CREATE TABLE IF NOT EXISTS target_state (
  target_id TEXT PRIMARY KEY,
  paused INTEGER NOT NULL DEFAULT 0,
  failures INTEGER NOT NULL DEFAULT 0,
  last_success REAL,
  last_http_status INTEGER,
  last_error TEXT,
  active_generation_id INTEGER,
  reserve_generation_id INTEGER
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_id TEXT,
  severity TEXT NOT NULL,
  kind TEXT NOT NULL,
  message TEXT NOT NULL,
  dedup_key TEXT,
  actor TEXT,
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS notification_state (
  dedup_key TEXT PRIMARY KEY,
  last_sent REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS operations (
  id TEXT PRIMARY KEY,
  target_id TEXT NOT NULL,
  action TEXT NOT NULL,
  status TEXT NOT NULL,
  actor TEXT,
  payload TEXT NOT NULL DEFAULT '{}',
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS locks (
  target_id TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  expires_at REAL NOT NULL
);
"""


class Database:
    def __init__(self, path: str):
        self.path = path

    async def initialize(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(SCHEMA)
            await db.commit()

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[aiosqlite.Connection]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            yield db

    async def ensure_target(self, target_id: str) -> None:
        async with self.connect() as db:
            await db.execute("INSERT OR IGNORE INTO target_state(target_id) VALUES (?)", (target_id,))
            await db.commit()

    async def reserve_generation(self, target_id: str, fqdn_factory) -> Generation:
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (await db.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM generations WHERE target_id=?",
                (target_id,),
            )).fetchone()
            sequence = int(row["sequence"])
            fqdn = fqdn_factory(sequence)
            now = time.time()
            cursor = await db.execute(
                "INSERT INTO generations(target_id,sequence,fqdn,state,created_at) VALUES (?,?,?,?,?)",
                (target_id, sequence, fqdn, RotationState.PREPARING.value, now),
            )
            generation_id = cursor.lastrowid
            await db.execute(
                "INSERT INTO target_state(target_id,reserve_generation_id) VALUES (?,?) "
                "ON CONFLICT(target_id) DO UPDATE SET reserve_generation_id=excluded.reserve_generation_id",
                (target_id, generation_id),
            )
            await db.commit()
        return await self.generation(generation_id)

    async def generation(self, generation_id: int) -> Generation:
        async with self.connect() as db:
            row = await (await db.execute("SELECT * FROM generations WHERE id=?", (generation_id,))).fetchone()
        if not row:
            raise KeyError(generation_id)
        return self._generation(row)

    async def generations(self, target_id: str | None = None) -> list[Generation]:
        query = "SELECT * FROM generations"
        params: tuple = ()
        if target_id:
            query += " WHERE target_id=?"
            params = (target_id,)
        query += " ORDER BY target_id, sequence DESC"
        async with self.connect() as db:
            rows = await (await db.execute(query, params)).fetchall()
        return [self._generation(row) for row in rows]

    async def active_and_reserve(self, target_id: str) -> tuple[Generation | None, Generation | None]:
        async with self.connect() as db:
            state = await (await db.execute("SELECT * FROM target_state WHERE target_id=?", (target_id,))).fetchone()
        if not state:
            return None, None
        active = await self.generation(state["active_generation_id"]) if state["active_generation_id"] else None
        reserve = await self.generation(state["reserve_generation_id"]) if state["reserve_generation_id"] else None
        return active, reserve

    async def update_generation(self, generation_id: int, **fields) -> Generation:
        allowed = {
            "state", "resource_id", "certificate_id", "provider_cname", "dns_record_id",
            "validation_record_id", "bytes_sent", "last_metric_ts", "activated_at", "drain_after", "metadata",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported fields: {unknown}")
        if "state" in fields and isinstance(fields["state"], RotationState):
            fields["state"] = fields["state"].value
        if "metadata" in fields:
            fields["metadata"] = json.dumps(fields["metadata"])
        sql = ",".join(f"{name}=?" for name in fields)
        async with self.connect() as db:
            await db.execute(f"UPDATE generations SET {sql} WHERE id=?", (*fields.values(), generation_id))
            await db.commit()
        return await self.generation(generation_id)

    async def activate(self, target_id: str, new_id: int, old_id: int | None, drain_after: float) -> None:
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                "UPDATE target_state SET active_generation_id=?,reserve_generation_id=NULL,failures=0,last_error=NULL WHERE target_id=?",
                (new_id, target_id),
            )
            await db.execute(
                "UPDATE generations SET state=?,activated_at=? WHERE id=?",
                (RotationState.ACTIVE.value, time.time(), new_id),
            )
            if old_id:
                await db.execute(
                    "UPDATE generations SET state=?,drain_after=? WHERE id=?",
                    (RotationState.DRAINING.value, drain_after, old_id),
                )
            await db.commit()

    async def import_active(self, target_id: str, resource_id: str, fqdn: str, bytes_sent: float = 0) -> Generation:
        generation = await self.reserve_generation(target_id, lambda _: fqdn)
        generation = await self.update_generation(
            generation.id, resource_id=resource_id, state=RotationState.ACTIVE, bytes_sent=bytes_sent,
            activated_at=time.time(),
        )
        async with self.connect() as db:
            await db.execute(
                "UPDATE target_state SET active_generation_id=?,reserve_generation_id=NULL WHERE target_id=?",
                (generation.id, target_id),
            )
            await db.commit()
        return generation

    async def target_states(self) -> list[dict]:
        async with self.connect() as db:
            rows = await (await db.execute("SELECT * FROM target_state ORDER BY target_id")).fetchall()
        return [dict(row) for row in rows]

    async def set_paused(self, target_id: str, paused: bool) -> None:
        await self.ensure_target(target_id)
        async with self.connect() as db:
            await db.execute("UPDATE target_state SET paused=? WHERE target_id=?", (int(paused), target_id))
            await db.commit()

    async def mark_check(self, target_id: str, success: bool, http_status: int | None, error: str | None) -> int:
        await self.ensure_target(target_id)
        async with self.connect() as db:
            if success:
                await db.execute(
                    "UPDATE target_state SET failures=0,last_success=?,last_http_status=?,last_error=NULL WHERE target_id=?",
                    (time.time(), http_status, target_id),
                )
            else:
                await db.execute(
                    "UPDATE target_state SET failures=failures+1,last_http_status=?,last_error=? WHERE target_id=?",
                    (http_status, error, target_id),
                )
            await db.commit()
            row = await (await db.execute("SELECT failures FROM target_state WHERE target_id=?", (target_id,))).fetchone()
        return int(row["failures"])

    async def add_event(self, target_id: str | None, severity: str, kind: str, message: str,
                        dedup_key: str | None = None, actor: str | None = None) -> int:
        async with self.connect() as db:
            cursor = await db.execute(
                "INSERT INTO events(target_id,severity,kind,message,dedup_key,actor,created_at) VALUES (?,?,?,?,?,?,?)",
                (target_id, severity, kind, message, dedup_key, actor, time.time()),
            )
            await db.commit()
            return int(cursor.lastrowid)

    async def claim_notification(self, dedup_key: str, dedup_seconds: int) -> bool:
        """Atomically claim a notification window so dedup survives restarts."""
        now = time.time()
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (await db.execute(
                "SELECT last_sent FROM notification_state WHERE dedup_key=?", (dedup_key,)
            )).fetchone()
            if row and now - float(row["last_sent"]) < dedup_seconds:
                await db.rollback()
                return False
            await db.execute(
                "INSERT INTO notification_state(dedup_key,last_sent) VALUES (?,?) "
                "ON CONFLICT(dedup_key) DO UPDATE SET last_sent=excluded.last_sent",
                (dedup_key, now),
            )
            await db.commit()
            return True

    async def events(self, target_id: str | None = None, limit: int = 50) -> list[dict]:
        query = "SELECT * FROM events"
        params: list = []
        if target_id:
            query += " WHERE target_id=?"
            params.append(target_id)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        async with self.connect() as db:
            rows = await (await db.execute(query, params)).fetchall()
        return [dict(row) for row in rows]

    async def acquire_lock(self, target_id: str, owner: str, ttl: int = 900) -> bool:
        now = time.time()
        async with self.connect() as db:
            await db.execute("DELETE FROM locks WHERE expires_at<?", (now,))
            try:
                await db.execute("INSERT INTO locks(target_id,owner,expires_at) VALUES (?,?,?)", (target_id, owner, now + ttl))
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def release_lock(self, target_id: str, owner: str) -> None:
        async with self.connect() as db:
            await db.execute("DELETE FROM locks WHERE target_id=? AND owner=?", (target_id, owner))
            await db.commit()

    @staticmethod
    def _generation(row) -> Generation:
        data = dict(row)
        data["metadata"] = json.loads(data["metadata"] or "{}")
        return Generation.model_validate(data)
