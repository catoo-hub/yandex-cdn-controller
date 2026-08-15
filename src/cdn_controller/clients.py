from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import jwt

from .models import TargetConfig


class ProviderError(RuntimeError):
    def __init__(self, provider: str, message: str, status_code: int | None = None, body: str = ""):
        body = body[:1000].strip()
        detail = f"; response={body}" if body else ""
        super().__init__(f"{provider}: {message}{detail}")
        self.provider = provider
        self.status_code = status_code
        self.body = body


class YandexIamAuth:
    AUDIENCE = "https://iam.api.cloud.yandex.net/iam/v1/tokens"

    def __init__(self, key_file: str, required: bool = True):
        self.key_file = key_file
        self.required = required
        self._key: dict[str, str] | None = None
        self._token = ""
        self._expires_at = 0.0
        self._lock = asyncio.Lock()
        self._http = httpx.AsyncClient(timeout=30)

    def _load_key(self) -> dict[str, str]:
        if self._key is not None:
            return self._key
        path = Path(self.key_file)
        if not path.is_file():
            if self.required:
                raise ProviderError("yandex-iam", f"authorized key file not found: {path}")
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ProviderError("yandex-iam", f"cannot read authorized key: {exc}") from exc
        missing = {"id", "service_account_id", "private_key"} - payload.keys()
        if missing:
            raise ProviderError("yandex-iam", f"authorized key is missing: {', '.join(sorted(missing))}")
        self._key = payload
        return payload

    async def headers(self) -> dict[str, str]:
        if self._token and time.time() < self._expires_at - 300:
            return {"Authorization": f"Bearer {self._token}"}

        async with self._lock:
            if self._token and time.time() < self._expires_at - 300:
                return {"Authorization": f"Bearer {self._token}"}
            key = self._load_key()
            if not key:
                return {}
            now = int(time.time())
            assertion = jwt.encode(
                {"aud": self.AUDIENCE, "iss": key["service_account_id"], "iat": now, "exp": now + 3600},
                key["private_key"], algorithm="PS256", headers={"kid": key["id"]},
            )
            try:
                response = await self._http.post(self.AUDIENCE, json={"jwt": assertion})
                response.raise_for_status()
                result = response.json()
            except (httpx.HTTPError, ValueError) as exc:
                raise ProviderError("yandex-iam", f"IAM token exchange failed: {exc}") from exc
            token = result.get("iamToken")
            if not token:
                raise ProviderError("yandex-iam", "IAM response has no iamToken")
            self._token = token
            # IAM tokens live up to 12h; refresh hourly even if expiresAt parsing changes.
            self._expires_at = time.time() + 3600
            return {"Authorization": f"Bearer {self._token}"}

    async def validate(self, exchange_token: bool = True) -> None:
        self._load_key()
        if exchange_token:
            await self.headers()

    async def close(self) -> None:
        await self._http.aclose()


class JsonClient:
    def __init__(self, provider: str, base_url: str, headers: dict[str, str], timeout: float = 30,
                 auth_headers=None):
        self.provider = provider
        self.client = httpx.AsyncClient(base_url=base_url, headers=headers, timeout=timeout)
        self.auth_headers = auth_headers

    async def request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                if self.auth_headers:
                    supplied = dict(kwargs.pop("headers", {}) or {})
                    kwargs["headers"] = {**await self.auth_headers(), **supplied}
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

    @staticmethod
    def _idempotency_key(value: str) -> str:
        """Return the supplied UUID or a stable UUID derived from a logical key."""
        try:
            return str(uuid.UUID(value))
        except ValueError:
            return str(uuid.uuid5(uuid.NAMESPACE_URL, f"yandex-cdn-controller:{value}"))

    def __init__(self, authorized_key_file: str, required: bool = True):
        self.auth = YandexIamAuth(authorized_key_file, required=required)
        headers = {"Content-Type": "application/json"}
        self.cdn = JsonClient("yandex-cdn", self.CDN, headers, auth_headers=self.auth.headers)
        self.cert = JsonClient("yandex-certificate-manager", self.CERT, headers, auth_headers=self.auth.headers)
        self.monitoring = JsonClient("yandex-monitoring", self.MONITORING, headers, auth_headers=self.auth.headers)
        self.operations = JsonClient("yandex-operations", self.OPERATIONS, headers, auth_headers=self.auth.headers)

    async def create_certificate(self, folder_id: str, fqdn: str, idempotency_key: str) -> dict:
        payload = {
            "folderId": folder_id,
            "name": fqdn.replace(".", "-"),
            "domains": [fqdn],
            "challengeType": "DNS",
            "deletionProtection": False,
        }
        return await self.cert.request(
            "POST", "/certificate-manager/v1/certificates/requestNew", json=payload,
            headers={"Idempotency-Key": self._idempotency_key(idempotency_key)},
        )

    async def get_certificate(self, certificate_id: str) -> dict:
        # Certificate Manager omits domain validation challenges in the BASIC
        # view. FULL is required so the controller can publish the DNS record.
        return await self.cert.request(
            "GET", f"/certificate-manager/v1/certificates/{certificate_id}",
            params={"view": "FULL"},
        )

    async def create_resource(self, target: TargetConfig, fqdn: str, certificate_id: str | None,
                              idempotency_key: str) -> dict:
        payload = {
            "folderId": target.yandex.folder_id,
            "cname": fqdn,
            "active": True,
            "originProtocol": target.yandex.origin_protocol,
            "origin": {"originGroupId": str(target.yandex.origin_group_id)},
            "providerType": "ourcdn",
            "sslCertificate": ({"type": "CM", "data": {"cm": {"id": certificate_id}}}
                               if certificate_id else {"type": "DONT_USE"}),
            "options": {
                "disableCache": {"enabled": True, "value": True},
                "browserCacheSettings": {"enabled": True, "value": "0"},
                "ignoreCookie": {"enabled": True, "value": False},
                "allowedHttpMethods": {"enabled": True, "value": ["GET", "HEAD", "OPTIONS"]},
                "hostOptions": {
                    "host": {"enabled": True, "value": target.yandex.origin_host_header}
                },
                "queryParamsOptions": {
                    "ignoreQueryString": {"enabled": True, "value": True}
                },
            },
        }
        return await self.cdn.request(
            "POST", "/cdn/v1/resources", json=payload,
            headers={"Idempotency-Key": self._idempotency_key(idempotency_key)},
        )

    async def delete_resource(self, resource_id: str, idempotency_key: str) -> dict:
        return await self.cdn.request(
            "DELETE", f"/cdn/v1/resources/{resource_id}",
            headers={"Idempotency-Key": self._idempotency_key(idempotency_key)},
        )

    async def recreate_resource(self, target: TargetConfig, snapshot: dict,
                                idempotency_key: str) -> dict:
        """Create a resource from the writable parts of a Resource.Get snapshot."""
        option_names = {
            "disableCache", "edgeCacheSettings", "browserCacheSettings", "cacheHttpHeaders",
            "queryParamsOptions", "slice", "compressionOptions", "redirectOptions", "hostOptions",
            "staticHeaders", "cors", "stale", "allowedHttpMethods", "proxyCacheMethodsSet",
            "disableProxyForceRanges", "staticRequestHeaders", "ignoreCookie", "rewrite",
            "secureKey", "ipAddressAcl", "followRedirects", "websockets", "headerFilter",
            "geoAcl", "referrerAcl", "staticResponse",
        }
        options = {key: value for key, value in (snapshot.get("options") or {}).items()
                   if key in option_names}
        ssl = snapshot.get("sslCertificate") or {}
        ssl_payload = {"type": ssl.get("type", "DONT_USE")}
        if ssl_payload["type"] == "CM":
            certificate_id = (((ssl.get("data") or {}).get("cm") or {}).get("id"))
            if not certificate_id:
                raise ProviderError("yandex-cdn", "CM certificate snapshot has no id")
            ssl_payload["data"] = {"cm": {"id": certificate_id}}
        origin_group_id = snapshot.get("originGroupId") or target.yandex.origin_group_id
        payload = {
            "folderId": target.yandex.folder_id,
            "cname": snapshot["cname"],
            "active": True,
            "originProtocol": snapshot.get("originProtocol") or target.yandex.origin_protocol,
            "origin": {"originGroupId": str(origin_group_id)},
            "providerType": snapshot.get("providerType") or "ourcdn",
            "sslCertificate": ssl_payload,
            "options": options,
        }
        if isinstance(snapshot.get("secondaryHostnames"), dict):
            payload["secondaryHostnames"] = snapshot["secondaryHostnames"]
        if snapshot.get("labels"):
            payload["labels"] = snapshot["labels"]
        if snapshot.get("tls"):
            payload["tls"] = snapshot["tls"]
        return await self.cdn.request(
            "POST", "/cdn/v1/resources", json=payload,
            headers={"Idempotency-Key": self._idempotency_key(idempotency_key)},
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
        await asyncio.gather(
            self.cdn.close(), self.cert.close(), self.monitoring.close(), self.operations.close(), self.auth.close()
        )


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

    async def validate_dns_access(self, zone_id: str) -> None:
        # DNS:Edit tokens scoped to one zone do not necessarily have Zone:Read.
        # Listing one DNS record exercises the same zone-scoped API used by upsert.
        result = await self.http.request(
            "GET", f"/zones/{zone_id}/dns_records", params={"per_page": 1}
        )
        if not result.get("success", True) or not isinstance(result.get("result"), list):
            raise ProviderError("cloudflare", json.dumps(result.get("errors", [])))

    async def delete_record(self, zone_id: str, record_id: str) -> None:
        await self.http.request("DELETE", f"/zones/{zone_id}/dns_records/{record_id}")

    async def close(self) -> None:
        await self.http.close()


class RemnawaveClient:
    def __init__(self, base_url: str, token: str, host_get_endpoint: str,
                 host_update_endpoint: str):
        self.host_get_endpoint = host_get_endpoint
        self.host_update_endpoint = host_update_endpoint
        self.http = JsonClient(
            "remnawave", base_url.rstrip("/"),
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )

    def get_path(self, host_id: str) -> str:
        return self.host_get_endpoint.format(host_id=host_id)

    @staticmethod
    def unwrap(payload: dict) -> dict:
        for key in ("response", "data", "host"):
            if isinstance(payload.get(key), dict):
                return payload[key]
        return payload

    async def get_host(self, host_id: str) -> dict:
        return self.unwrap(await self.http.request("GET", self.get_path(host_id)))

    async def patch_host(self, host_id: str, current: dict, fqdn: str, fields: list[str]) -> dict:
        payload = {"uuid": host_id}
        aliases = {"address": "address", "sni": "sni", "host": "host"}
        for field in fields:
            if field not in aliases:
                raise ProviderError("remnawave", f"unsupported update field {field}")
            payload[aliases[field]] = fqdn
        return self.unwrap(await self.http.request("PATCH", self.host_update_endpoint, json=payload))

    async def restore_host(self, host_id: str, previous: dict, fields: list[str]) -> dict:
        payload = {"uuid": host_id}
        for field in fields:
            if field not in {"address", "sni", "host"}:
                raise ProviderError("remnawave", f"unsupported restore field {field}")
            payload[field] = previous.get(field)
        return self.unwrap(await self.http.request("PATCH", self.host_update_endpoint, json=payload))

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
    return RemnawaveClient(
        provider.base_url, token, provider.host_get_endpoint, provider.host_update_endpoint
    )
