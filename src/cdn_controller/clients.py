from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from typing import Any

import httpx

from .models import TargetConfig


class ProviderError(RuntimeError):
    def __init__(self, provider: str, message: str, status_code: int | None = None, body: str = ""):
        super().__init__(f"{provider}: {message}")
        self.provider = provider
        self.status_code = status_code
        self.body = body[:1000]


class JsonClient:
    def __init__(self, provider: str, base_url: str, headers: dict[str, str], timeout: float = 30):
        self.provider = provider
        self.client = httpx.AsyncClient(base_url=base_url, headers=headers, timeout=timeout)

    async def request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                response = await self.client.request(method, path, **kwargs)
                if response.status_code in {429, 500, 502, 503, 504} and attempt < 3:
                    await asyncio.sleep(2 ** attempt)
                    continue
                if response.is_error:
                    raise ProviderError(self.provider, f"HTTP {response.status_code}", response.status_code, response.text)
                return response.json() if response.content else {}
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = exc
                if attempt < 3:
                    await asyncio.sleep(2 ** attempt)
                    continue
        raise ProviderError(self.provider, str(last_error or "request failed"))

    async def close(self) -> None:
        await self.client.aclose()


class YandexClient:
    CDN = "https://cdn.api.cloud.yandex.net"
    CERT = "https://certificate-manager.api.cloud.yandex.net"
    MONITORING = "https://monitoring.api.cloud.yandex.net"
    OPERATIONS = "https://operation.api.cloud.yandex.net"

    def __init__(self, api_key: str):
        headers = {"Authorization": f"Api-Key {api_key}", "Content-Type": "application/json"}
        self.cdn = JsonClient("yandex-cdn", self.CDN, headers)
        self.cert = JsonClient("yandex-certificate-manager", self.CERT, headers)
        self.monitoring = JsonClient("yandex-monitoring", self.MONITORING, headers)
        self.operations = JsonClient("yandex-operations", self.OPERATIONS, headers)

    async def create_certificate(self, folder_id: str, fqdn: str, idempotency_key: str) -> dict:
        payload = {
            "folderId": folder_id,
            "name": fqdn.replace(".", "-"),
            "domains": [fqdn],
            "challengeType": "DNS",
            "deletionProtection": False,
        }
        return await self.cert.request(
            "POST", "/certificate-manager/v1/certificates:requestNew", json=payload,
            headers={"Idempotency-Key": idempotency_key},
        )

    async def get_certificate(self, certificate_id: str) -> dict:
        return await self.cert.request("GET", f"/certificate-manager/v1/certificates/{certificate_id}")

    async def create_resource(self, target: TargetConfig, fqdn: str, certificate_id: str,
                              idempotency_key: str) -> dict:
        payload = {
            "folderId": target.yandex.folder_id,
            "cname": fqdn,
            "active": True,
            "originProtocol": target.yandex.origin_protocol,
            "origin": {"originGroupId": str(target.yandex.origin_group_id)},
            "providerType": "ourcdn",
            "sslCertificate": {
                "type": "CM",
                "data": {"cm": {"id": certificate_id}},
            },
            "options": {
                "disableCache": {"enabled": True, "value": True},
                "allowedHttpMethods": {"enabled": True, "value": ["GET", "HEAD"]},
                "hostOptions": {
                    "host": {"enabled": True, "value": target.yandex.origin_host_header}
                },
                "queryParamsOptions": {
                    "ignoreQueryString": {"enabled": True, "value": False}
                },
            },
        }
        return await self.cdn.request(
            "POST", "/cdn/v1/resources", json=payload,
            headers={"Idempotency-Key": idempotency_key},
        )

    async def get_resource(self, resource_id: str) -> dict:
        return await self.cdn.request("GET", f"/cdn/v1/resources/{resource_id}")

    async def update_resource_active(self, resource_id: str, active: bool) -> dict:
        return await self.cdn.request(
            "PATCH", f"/cdn/v1/resources/{resource_id}?updateMask=active", json={"active": active}
        )

    async def read_bytes_sent(self, folder_id: str, resource_id: str, from_time: str, to_time: str) -> dict:
        payload = {
            "query": f'\"edge.bytes_sent\"{{service=\"yccdn\", resource=\"{resource_id}\"}}',
            "fromTime": from_time,
            "toTime": to_time,
            "downsampling": {"disabled": True, "gridAggregation": "AVG", "gapFilling": "NONE"},
        }
        return await self.monitoring.request(
            "POST", f"/monitoring/v2/data/read?folderId={folder_id}", json=payload
        )

    async def wait_operation(self, operation: dict, timeout_seconds: int = 900) -> dict:
        operation_id = operation.get("id")
        if not operation_id:
            raise ProviderError("yandex-operations", "operation has no id")
        deadline = time.monotonic() + timeout_seconds
        current = operation
        while not current.get("done") and time.monotonic() < deadline:
            await asyncio.sleep(2)
            current = await self.operations.request("GET", f"/operations/{operation_id}")
        if not current.get("done"):
            raise ProviderError("yandex-operations", f"operation {operation_id} timed out")
        if current.get("error"):
            raise ProviderError("yandex-operations", json.dumps(current["error"]))
        if not isinstance(current.get("response"), dict):
            raise ProviderError("yandex-operations", f"operation {operation_id} has no response")
        return current

    async def close(self) -> None:
        await asyncio.gather(self.cdn.close(), self.cert.close(), self.monitoring.close(), self.operations.close())


class CloudflareClient:
    def __init__(self, token: str):
        self.http = JsonClient(
            "cloudflare", "https://api.cloudflare.com/client/v4",
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )

    async def upsert_record(self, zone_id: str, record_type: str, name: str, content: str,
                            comment: str) -> str:
        found = await self.http.request(
            "GET", f"/zones/{zone_id}/dns_records", params={"type": record_type, "name": name}
        )
        records = found.get("result", [])
        payload = {
            "type": record_type, "name": name, "content": content,
            "ttl": 60, "proxied": False, "comment": comment,
        }
        if records:
            record_id = records[0]["id"]
            result = await self.http.request("PUT", f"/zones/{zone_id}/dns_records/{record_id}", json=payload)
        else:
            result = await self.http.request("POST", f"/zones/{zone_id}/dns_records", json=payload)
        if not result.get("success", True):
            raise ProviderError("cloudflare", json.dumps(result.get("errors", [])))
        return result["result"]["id"]

    async def delete_record(self, zone_id: str, record_id: str) -> None:
        await self.http.request("DELETE", f"/zones/{zone_id}/dns_records/{record_id}")

    async def close(self) -> None:
        await self.http.close()


class RemnawaveClient:
    def __init__(self, base_url: str, token: str, host_endpoint: str):
        self.host_endpoint = host_endpoint
        self.http = JsonClient(
            "remnawave", base_url.rstrip("/"),
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )

    def path(self, host_id: str) -> str:
        return self.host_endpoint.format(host_id=host_id)

    @staticmethod
    def unwrap(payload: dict) -> dict:
        for key in ("response", "data", "host"):
            if isinstance(payload.get(key), dict):
                return payload[key]
        return payload

    async def get_host(self, host_id: str) -> dict:
        return self.unwrap(await self.http.request("GET", self.path(host_id)))

    async def patch_host(self, host_id: str, current: dict, fqdn: str, fields: list[str]) -> dict:
        payload = dict(current)
        aliases = {"address": "address", "sni": "sni", "host": "host"}
        for field in fields:
            if field not in aliases:
                raise ProviderError("remnawave", f"unsupported update field {field}")
            payload[aliases[field]] = fqdn
        return self.unwrap(await self.http.request("PATCH", self.path(host_id), json=payload))

    async def close(self) -> None:
        await self.http.close()


@dataclass
class HealthResult:
    healthy: bool
    root_status: int | None
    path_status: int | None
    error: str | None = None
    administrative: bool = False


async def check_cdn_health(fqdn: str, path: str, expected_root: int, expected_path: int,
                           port: int = 443) -> HealthResult:
    try:
        authority = fqdn if port == 443 else f"{fqdn}:{port}"
        async with httpx.AsyncClient(timeout=15, follow_redirects=False, http2=True) as client:
            root, transport = await asyncio.gather(
                client.get(f"https://{authority}/"), client.get(f"https://{authority}{path}")
            )
        administrative = 451 in {root.status_code, transport.status_code}
        healthy = root.status_code == expected_root and transport.status_code == expected_path
        return HealthResult(healthy, root.status_code, transport.status_code,
                            None if healthy else "unexpected HTTP status", administrative)
    except (httpx.HTTPError, OSError) as exc:
        return HealthResult(False, None, None, str(exc), False)


def webhook_signature(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def provider_operation_id(payload: dict) -> str | None:
    operation = payload.get("operation") if isinstance(payload.get("operation"), dict) else payload
    return operation.get("id") if isinstance(operation, dict) else None


def extract_resource(payload: dict) -> dict:
    if isinstance(payload.get("response"), dict):
        return payload["response"]
    if isinstance(payload.get("resource"), dict):
        return payload["resource"]
    return payload


def remnawave_client(provider, token_env: str) -> RemnawaveClient:
    token = os.getenv(token_env, "")
    if not token:
        raise ProviderError("remnawave", f"environment variable {token_env} is empty")
    return RemnawaveClient(provider.base_url, token, provider.host_endpoint)
