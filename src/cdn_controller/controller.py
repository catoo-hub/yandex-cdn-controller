from __future__ import annotations

import asyncio
import json
import logging
import shutil
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .clients import (
    CloudflareClient, ProviderError, YandexClient, check_cdn_health,
    extract_resource, remnawave_client,
)
from .db import Database
from .metrics import (
    BYTES_SENT, HTTP_STATUS, LAST_SUCCESS, PROVIDER_ERRORS, RESOURCE_STATUS,
    ROTATION_STATE, ROTATIONS, TARGET_HEALTHY, TRAFFIC_RATIO,
)
from .models import AppConfig, Generation, RotationState, TargetConfig
from .notifications import Event, Notifier
from .settings import Settings
from .traffic import extract_series, integrate_rate_series

log = logging.getLogger(__name__)
GIB = 1024 ** 3


class Controller:
    def __init__(self, config: AppConfig, settings: Settings, db: Database):
        self.config = config
        self.settings = settings
        self.db = db
        self.yandex = YandexClient(settings.yandex_authorized_key_file, required=not settings.dry_run)
        self.cloudflare = CloudflareClient(settings.cloudflare_api_token)
        self.notifier = Notifier(db, settings, config.notification_dedup_seconds)
        self.running = False
        self.scheduler_heartbeat = 0.0

    async def initialize(self) -> None:
        await self.db.initialize()
        for target in self.config.targets:
            await self.db.ensure_target(target.id)

    async def close(self) -> None:
        await asyncio.gather(self.yandex.close(), self.cloudflare.close())

    async def run(self) -> None:
        self.running = True
        while self.running:
            started = time.monotonic()
            await asyncio.gather(*(self.reconcile(target.id) for target in self.config.targets if target.enabled))
            await self.deactivate_drained()
            self.scheduler_heartbeat = time.time()
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(1, self.config.poll_interval_seconds - elapsed))

    async def reconcile(self, target_id: str, actor: str = "scheduler") -> dict:
        target = self.config.target(target_id)
        owner = f"reconcile:{uuid.uuid4()}"
        if not await self.db.acquire_lock(target_id, owner):
            return {"target": target_id, "status": "locked"}
        try:
            states = {row["target_id"]: row for row in await self.db.target_states()}
            if states[target_id]["paused"]:
                return {"target": target_id, "status": "paused"}
            active, reserve = await self.db.active_and_reserve(target_id)
            if active:
                active = await self._refresh_traffic(target, active)
                health = await check_cdn_health(
                    active.fqdn, target.transport.path,
                    target.transport.expected_root_status, target.transport.expected_path_status,
                    target.transport.port,
                )
                failures = await self._record_health(target, active, health)
                if health.administrative:
                    await self.db.update_generation(active.id, state=RotationState.ATTENTION)
                    await self.notifier.emit(Event(target_id, "CRITICAL", "administrative-block",
                                                   f"{active.fqdn} returned HTTP 451; automatic rotation stopped"))
                    return {"target": target_id, "status": "attention"}
                threshold = target.rotation.switch_at_gib * GIB
                prepare = target.rotation.prepare_at_gib * GIB
                await self._traffic_notifications(target, active)
                if (not health.healthy and failures >= target.rotation.technical_failures_before_prepare
                        and not reserve):
                    reserve = await self.prepare(target_id, actor="technical-health", lock_held=True)
                if active.bytes_sent >= prepare and not reserve:
                    reserve = await self.prepare(target_id, actor=actor, lock_held=True)
                simulated = bool(reserve and reserve.metadata.get("rotation_simulated"))
                if (active.bytes_sent >= threshold and reserve and reserve.state == RotationState.READY
                        and not (self.settings.dry_run and simulated)):
                    await self.rotate(target_id, actor=actor, lock_held=True)
            return {"target": target_id, "status": "ok", "active": active.model_dump() if active else None,
                    "reserve": reserve.model_dump() if reserve else None}
        except ProviderError as exc:
            PROVIDER_ERRORS.labels(exc.provider).inc()
            await self.notifier.emit(Event(target_id, "ERROR", "provider-error", str(exc)))
            return {"target": target_id, "status": "error", "error": str(exc)}
        finally:
            await self.db.release_lock(target_id, owner)

    async def _refresh_traffic(self, target: TargetConfig, generation: Generation) -> Generation:
        if not generation.resource_id or self.settings.dry_run:
            self._publish_generation(target, generation)
            return generation
        end = datetime.now(UTC)
        start = datetime.fromtimestamp(generation.last_metric_ts, UTC) - timedelta(minutes=10) \
            if generation.last_metric_ts else max(datetime.fromtimestamp(generation.created_at, UTC), end - timedelta(days=30))
        payload = await self.yandex.read_bytes_sent(
            target.yandex.folder_id, generation.resource_id, start.isoformat(), end.isoformat()
        )
        timestamps, values = extract_series(payload)
        increment, checkpoint = integrate_rate_series(timestamps, values, generation.last_metric_ts)
        generation = await self.db.update_generation(
            generation.id, bytes_sent=generation.bytes_sent + increment, last_metric_ts=checkpoint
        )
        self._publish_generation(target, generation)
        return generation

    async def _record_health(self, target, generation, health) -> int:
        prior = next((row for row in await self.db.target_states() if row["target_id"] == target.id), {})
        failures = await self.db.mark_check(target.id, health.healthy, health.root_status, health.error)
        TARGET_HEALTHY.labels(target.id).set(int(health.healthy))
        HTTP_STATUS.labels(target.id).set(health.root_status or 0)
        if health.healthy:
            LAST_SUCCESS.labels(target.id).set(time.time())
            if int(prior.get("failures") or 0) > 0:
                await self.notifier.emit(Event(
                    target.id, "INFO", "health-recovered", f"{generation.fqdn} recovered",
                    dedup_key=f"{target.id}:{generation.id}:recovered:{int(time.time() // 1800)}",
                ))
        await self.notifier.kuma(
            target.monitoring.uptime_kuma_push_url or self.settings.uptime_kuma_push_url,
            health.healthy, "healthy" if health.healthy else (health.error or "unhealthy"),
        )
        if not health.healthy and failures >= target.rotation.technical_failures_before_prepare:
            await self.notifier.emit(Event(target.id, "WARNING", "health-failure",
                                           f"{generation.fqdn}: {health.error}; failures={failures}"))
        return failures

    async def _traffic_notifications(self, target: TargetConfig, generation: Generation) -> None:
        switch = target.rotation.switch_at_gib * GIB
        milestones = (
            ("traffic-70", switch * 0.70, "INFO", "70% of switch threshold reached"),
            ("traffic-90", switch * 0.90, "WARNING", "90% of switch threshold reached"),
            ("traffic-prepare", target.rotation.prepare_at_gib * GIB, "WARNING", "prepare threshold reached"),
            ("traffic-switch", switch, "CRITICAL", "switch threshold reached"),
        )
        for kind, value, severity, message in milestones:
            if generation.bytes_sent >= value:
                await self.notifier.emit(Event(
                    target.id, severity, kind,
                    f"{message}: {generation.bytes_sent / GIB:.2f} GiB",
                    dedup_key=f"{target.id}:{generation.id}:{kind}",
                ))

    def _publish_generation(self, target: TargetConfig, generation: Generation) -> None:
        BYTES_SENT.labels(target.id, str(generation.sequence)).set(generation.bytes_sent)
        TRAFFIC_RATIO.labels(target.id, str(generation.sequence)).set(
            generation.bytes_sent / (target.rotation.switch_at_gib * GIB)
        )
        RESOURCE_STATUS.labels(target.id, str(generation.sequence), generation.state.value).set(1)
        ROTATION_STATE.labels(target.id, generation.state.value).set(1)

    async def prepare(self, target_id: str, actor: str = "cli", lock_held: bool = False) -> Generation:
        target = self.config.target(target_id)
        owner = f"prepare:{uuid.uuid4()}"
        acquired = lock_held or await self.db.acquire_lock(target_id, owner)
        if not acquired:
            raise RuntimeError("target is locked")
        try:
            _, existing = await self.db.active_and_reserve(target_id)
            if existing and existing.state == RotationState.READY:
                return existing
            generation = existing or await self.db.reserve_generation(target_id, target.domain.render)
            if not existing:
                await self.notifier.emit(Event(target_id, "INFO", "prepare-started",
                                               f"Preparing {generation.fqdn}", actor=actor))
            if self.settings.dry_run:
                generation = await self.db.update_generation(
                    generation.id, state=RotationState.READY,
                    certificate_id=f"dry-cert-{generation.sequence}",
                    resource_id=f"dry-resource-{generation.sequence}",
                    provider_cname=f"dry-{generation.sequence}.topology.gslb.yccdn.ru",
                    metadata={"dry_run": True},
                )
                await self.notifier.emit(Event(target_id, "INFO", "prepare-ready",
                                               f"DRY-RUN reserve ready: {generation.fqdn}", actor=actor))
                return generation

            if generation.certificate_id:
                cert_id = generation.certificate_id
                certificate = await self.yandex.get_certificate(cert_id)
            else:
                cert_operation = await self.yandex.create_certificate(
                    target.yandex.folder_id, generation.fqdn, f"cert:{target.id}:{generation.sequence}"
                )
                cert_result = await self.yandex.wait_operation(cert_operation)
                certificate = cert_result["response"]
                cert_id = str(certificate["id"])
                generation = await self.db.update_generation(
                    generation.id, certificate_id=cert_id,
                    metadata={**generation.metadata, "certificate_operation": cert_operation.get("id")},
                )
            if str(certificate.get("status", "")).upper() != "ISSUED":
                if not certificate.get("challenges"):
                    certificate = await self._wait_certificate(cert_id)
                if not generation.validation_record_id:
                    challenge = self._dns_challenge(certificate)
                    validation_id = await self.cloudflare.upsert_record(
                        target.domain.cloudflare_zone_id, challenge["type"], challenge["name"], challenge["value"],
                        f"cdn-controller validation {target.id}/{generation.sequence}",
                    )
                    generation = await self.db.update_generation(
                        generation.id, validation_record_id=validation_id,
                    )
                await self._wait_certificate(cert_id, issued=True)

            if generation.resource_id:
                resource_id = generation.resource_id
            else:
                resource_operation = await self.yandex.create_resource(
                    target, generation.fqdn, cert_id, f"cdn:{target.id}:{generation.sequence}"
                )
                resource_result = await self.yandex.wait_operation(resource_operation)
                resource = extract_resource(resource_result)
                resource_id = str(resource["id"])
                generation = await self.db.update_generation(
                    generation.id, resource_id=resource_id,
                    metadata={**generation.metadata, "resource_operation": resource_operation.get("id")},
                )
            resource = await self._wait_resource(resource_id)
            provider_cname = resource.get("providerCname") or resource.get("provider_cname")
            if not provider_cname:
                raise ProviderError("yandex-cdn", "resource response has no provider_cname")
            if not generation.dns_record_id:
                dns_id = await self.cloudflare.upsert_record(
                    target.domain.cloudflare_zone_id, "CNAME", generation.fqdn, provider_cname,
                    f"cdn-controller target {target.id}/{generation.sequence}",
                )
                generation = await self.db.update_generation(
                    generation.id, provider_cname=provider_cname, dns_record_id=dns_id,
                )
            health = await self._wait_health(target, generation.fqdn)
            if not health.healthy:
                raise ProviderError("health", health.error or "reserve is unhealthy", health.root_status)
            generation = await self.db.update_generation(generation.id, state=RotationState.READY)
            await self.notifier.emit(Event(target_id, "INFO", "prepare-ready",
                                           f"Reserve ready: {generation.fqdn}", actor=actor))
            return generation
        finally:
            if not lock_held:
                await self.db.release_lock(target_id, owner)

    async def rotate(self, target_id: str, actor: str = "cli", lock_held: bool = False) -> dict:
        target = self.config.target(target_id)
        owner = f"rotate:{uuid.uuid4()}"
        acquired = lock_held or await self.db.acquire_lock(target_id, owner)
        if not acquired:
            raise RuntimeError("target is locked")
        try:
            active, reserve = await self.db.active_and_reserve(target_id)
            if not reserve or reserve.state != RotationState.READY:
                raise RuntimeError("no READY reserve")
            if active and active.state == RotationState.ATTENTION:
                raise RuntimeError("administratively restricted target requires manual investigation")
            if self.settings.dry_run:
                reserve = await self.db.update_generation(
                    reserve.id,
                    metadata={**reserve.metadata, "rotation_simulated": True, "simulated_at": time.time()},
                )
                await self.notifier.emit(Event(
                    target_id, "INFO", "rotation-dry-run",
                    f"DRY-RUN: would switch Remnawave from "
                    f"{active.fqdn if active else 'none'} to {reserve.fqdn}; no state or provider changes made",
                    dedup_key=f"{target_id}:{reserve.id}:rotation-dry-run", actor=actor,
                ))
                return {
                    "dry_run": True,
                    "changed": False,
                    "old": active.fqdn if active else None,
                    "new": reserve.fqdn,
                }
            await self.backup_database()
            provider = self.config.providers.remnawave[target.remnawave.panel]
            client = remnawave_client(provider, provider.token_env)
            try:
                previous = await client.get_host(target.remnawave.host_id)
                await client.patch_host(target.remnawave.host_id, previous, reserve.fqdn,
                                        target.remnawave.update_fields)
                updated = await client.get_host(target.remnawave.host_id)
                for field in target.remnawave.update_fields:
                    if updated.get(field) != reserve.fqdn:
                        raise ProviderError("remnawave", f"field {field} was not updated")
                health = await check_cdn_health(
                    reserve.fqdn, target.transport.path,
                    target.transport.expected_root_status, target.transport.expected_path_status,
                    target.transport.port,
                )
                if not health.healthy:
                    await client.restore_host(
                        target.remnawave.host_id, previous, target.remnawave.update_fields
                    )
                    ROTATIONS.labels(target_id, "rollback").inc()
                    raise ProviderError("health", "post-switch health check failed")
            finally:
                await client.close()
            drain_after = time.time() + self.config.drain_hours * 3600
            await self.db.activate(target_id, reserve.id, active.id if active else None, drain_after)
            ROTATIONS.labels(target_id, "success").inc()
            await self.notifier.emit(Event(target_id, "INFO", "rotation-complete",
                                           f"Remnawave switched to {reserve.fqdn}", actor=actor))
            return {"old": active.fqdn if active else None, "new": reserve.fqdn, "previous_host": previous}
        except Exception:
            ROTATIONS.labels(target_id, "error").inc()
            raise
        finally:
            if not lock_held:
                await self.db.release_lock(target_id, owner)

    async def rollback(self, target_id: str, actor: str = "cli") -> dict:
        generations = await self.db.generations(target_id)
        current = next((g for g in generations if g.state == RotationState.ACTIVE), None)
        previous = next((g for g in generations if g.state == RotationState.DRAINING), None)
        if not current or not previous:
            raise RuntimeError("no active/draining pair to roll back")
        target = self.config.target(target_id)
        if not self.settings.dry_run:
            provider = self.config.providers.remnawave[target.remnawave.panel]
            client = remnawave_client(provider, provider.token_env)
            try:
                host = await client.get_host(target.remnawave.host_id)
                await client.patch_host(target.remnawave.host_id, host, previous.fqdn, target.remnawave.update_fields)
            finally:
                await client.close()
        await self.db.activate(target_id, previous.id, current.id, time.time() + self.config.drain_hours * 3600)
        await self.notifier.emit(Event(target_id, "WARNING", "rollback-complete",
                                       f"Rolled back to {previous.fqdn}", actor=actor))
        return {"active": previous.fqdn}

    async def deactivate_drained(self) -> list[str]:
        changed = []
        for generation in await self.db.generations():
            if generation.state == RotationState.DRAINING and generation.drain_after and generation.drain_after <= time.time():
                if not self.settings.dry_run and generation.resource_id:
                    await self.yandex.update_resource_active(generation.resource_id, False)
                await self.db.update_generation(generation.id, state=RotationState.RETIRED)
                changed.append(generation.fqdn)
        return changed

    async def backup_database(self) -> str:
        destination = f"{self.settings.database_path}.{int(time.time())}.bak"
        if Path(self.settings.database_path).exists():
            shutil.copy2(self.settings.database_path, destination)
        return destination

    async def status(self) -> dict:
        return {
            "dry_run": self.settings.dry_run,
            "scheduler_heartbeat": self.scheduler_heartbeat,
            "targets": await self.db.target_states(),
            "generations": [g.model_dump(mode="json") for g in await self.db.generations()],
        }

    async def _wait_certificate(self, certificate_id: str, issued: bool = False) -> dict:
        for _ in range(60):
            cert = await self.yandex.get_certificate(certificate_id)
            status = str(cert.get("status", "")).upper()
            if issued and status == "ISSUED":
                return cert
            if not issued and cert.get("challenges"):
                return cert
            if status in {"INVALID", "REVOKED"}:
                raise ProviderError("yandex-certificate-manager", f"certificate status {status}")
            await asyncio.sleep(10)
        raise ProviderError("yandex-certificate-manager", "certificate timeout")

    async def _wait_resource(self, resource_id: str) -> dict:
        for _ in range(90):
            resource = await self.yandex.get_resource(resource_id)
            status = str(resource.get("status", "")).upper()
            if status in {"READY", "ACTIVE"}:
                return resource
            # Current ourcdn Resource.Get has no top-level status. A completed
            # resource is identifiable by its active flag, provider CNAME and
            # attached Certificate Manager certificate in READY state.
            ssl = resource.get("sslCertificate") or {}
            if (resource.get("active") is True and resource.get("providerCname")
                    and str(ssl.get("status", "")).upper() == "READY"):
                return resource
            if status in {"ERROR", "FAILED", "SUSPENDED"}:
                raise ProviderError("yandex-cdn", f"resource status {status}")
            await asyncio.sleep(10)
        raise ProviderError("yandex-cdn", "resource processing timeout")

    async def _wait_health(self, target: TargetConfig, fqdn: str):
        result = None
        # CDN settings and DNS propagation may take up to 15 minutes.
        for _ in range(90):
            result = await check_cdn_health(
                fqdn, target.transport.path,
                target.transport.expected_root_status, target.transport.expected_path_status,
                target.transport.port,
            )
            if result.healthy or result.administrative:
                return result
            await asyncio.sleep(10)
        return result

    @staticmethod
    def _extract_id(payload: dict) -> str:
        for candidate in (payload, payload.get("metadata", {}), payload.get("response", {})):
            if isinstance(candidate, dict) and candidate.get("id"):
                return str(candidate["id"])
        raise ProviderError("yandex", "operation response has no id")

    @staticmethod
    def _dns_challenge(certificate: dict) -> dict:
        challenges = certificate.get("challenges", [])
        if not challenges:
            raise ProviderError("yandex-certificate-manager", "no DNS challenge returned")
        challenge = challenges[0]
        dns = challenge.get("dnsChallenge") or challenge.get("dns_challenge") or challenge
        record_type = str(dns.get("type") or "TXT").upper()
        if record_type not in {"TXT", "CNAME"}:
            record_type = "CNAME" if "CNAME" in record_type else "TXT"
        name = dns.get("dnsName") or dns.get("dns_name") or dns.get("name")
        value = dns.get("dnsValue") or dns.get("dns_value") or dns.get("value")
        if not name or not value:
            raise ProviderError("yandex-certificate-manager", "invalid DNS challenge")
        return {"type": record_type, "name": name, "value": value}
