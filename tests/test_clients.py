import json
import pytest
import respx
import uuid
from httpx import Response

from cdn_controller.clients import (
    CloudflareClient, ProviderError, RemnawaveClient, YandexClient, YandexIamAuth, webhook_signature,
)


@respx.mock
@pytest.mark.asyncio
async def test_get_certificate_requests_full_view():
    route = respx.get(
        "https://certificate-manager.api.cloud.yandex.net/certificate-manager/v1/certificates/cert-id",
        params={"view": "FULL"},
    ).mock(return_value=Response(200, json={"id": "cert-id", "challenges": []}))
    client = YandexClient("", required=False)
    try:
        result = await client.get_certificate("cert-id")
        assert result["id"] == "cert-id"
        assert route.called
    finally:
        await client.close()
from cdn_controller.models import TargetConfig


def test_webhook_signature_is_stable():
    assert webhook_signature("secret", b"body") == "dc46983557fea127b43af721467eb9b3fde2338fe3e14f51952aa8478c13d355"


def test_provider_error_includes_bounded_response_body():
    error = ProviderError("yandex-cdn", "HTTP 400", 400, '{"message":"invalid field"}')
    assert str(error) == 'yandex-cdn: HTTP 400; response={"message":"invalid field"}'
    assert error.body == '{"message":"invalid field"}'


@pytest.mark.asyncio
@respx.mock
async def test_certificate_request_uses_documented_rest_path():
    route = respx.post(
        "https://certificate-manager.api.cloud.yandex.net/"
        "certificate-manager/v1/certificates/requestNew"
    ).mock(return_value=Response(200, json={"id": "operation-1"}))
    client = YandexClient("", required=False)
    try:
        await client.create_certificate("folder", "yc-001.example.com", "idem")
    finally:
        await client.close()
    assert route.called
    key = route.calls[0].request.headers["Idempotency-Key"]
    assert str(uuid.UUID(key)) == key
    assert key == YandexClient._idempotency_key("idem")


@pytest.mark.asyncio
@respx.mock
async def test_yandex_create_resource_uses_current_nested_schema():
    route = respx.post("https://cdn.api.cloud.yandex.net/cdn/v1/resources").mock(
        return_value=Response(200, json={"id": "operation-1"})
    )
    target = TargetConfig.model_validate({
        "id": "de", "yandex": {"folder_id": "f", "origin_group_id": 42,
        "origin_host_header": "origin.example.com"},
        "domain": {"zone": "example.com", "pattern": "yc-{sequence:03d}.example.com",
        "cloudflare_zone_id": "z"},
        "remnawave": {"panel": "primary", "host_id": "h"},
        "transport": {"path": "/x/"},
    })
    client = YandexClient("", required=False)
    try:
        await client.create_resource(target, "yc-001.example.com", "cert-1", "idem")
    finally:
        await client.close()
    body = route.calls[0].request.content.decode()
    assert '"origin":{"originGroupId":"42"}' in body
    assert '"data":{"cm":{"id":"cert-1"}}' in body
    assert '"disableCache":{"enabled":true,"value":true}' in body
    assert '"browserCacheSettings":{"enabled":true,"value":"0"}' in body
    assert '"ignoreCookie":{"enabled":true,"value":false}' in body
    assert '"allowedHttpMethods":{"enabled":true,"value":["GET","HEAD","OPTIONS"]}' in body
    assert '"ignoreQueryString":{"enabled":true,"value":false}' in body
    key = route.calls[0].request.headers["Idempotency-Key"]
    assert str(uuid.UUID(key)) == key
    assert key == YandexClient._idempotency_key("idem")


@pytest.mark.asyncio
@respx.mock
async def test_yandex_resource_without_certificate_and_delete_use_uuid_keys():
    create = respx.post("https://cdn.api.cloud.yandex.net/cdn/v1/resources").mock(
        return_value=Response(200, json={"id": "create-operation"})
    )
    delete = respx.delete("https://cdn.api.cloud.yandex.net/cdn/v1/resources/resource-id").mock(
        return_value=Response(200, json={"id": "delete-operation"})
    )
    target = TargetConfig.model_validate({
        "id": "direct", "yandex": {"folder_id": "f", "origin_group_id": 42,
        "origin_protocol": "HTTP", "origin_host_header": "203.0.113.10"},
        "transport": {"path": "/x/"},
        "rotation": {"mode": "recreate_in_place", "recreate_at_gib": 740},
    })
    client = YandexClient("", required=False)
    try:
        await client.create_resource(target, "account.yccdn.example", None, "create-logical")
        await client.delete_resource("resource-id", "delete-logical")
    finally:
        await client.close()
    assert '"sslCertificate":{"type":"DONT_USE"}' in create.calls[0].request.content.decode()
    assert str(uuid.UUID(create.calls[0].request.headers["Idempotency-Key"]))
    assert str(uuid.UUID(delete.calls[0].request.headers["Idempotency-Key"]))


@pytest.mark.asyncio
@respx.mock
async def test_recreate_resource_copies_writable_snapshot_and_drops_read_only_options():
    route = respx.post("https://cdn.api.cloud.yandex.net/cdn/v1/resources").mock(
        return_value=Response(200, json={"id": "operation"})
    )
    target = TargetConfig.model_validate({
        "id": "direct", "yandex": {"folder_id": "f", "origin_group_id": 42,
        "origin_protocol": "HTTP", "origin_host_header": "203.0.113.10"},
        "transport": {"path": "/x/"},
        "rotation": {"mode": "recreate_in_place", "recreate_at_gib": 740},
    })
    snapshot = {
        "cname": "resource.example", "originGroupId": "99", "originProtocol": "HTTPS",
        "providerType": "ourcdn",
        "sslCertificate": {"type": "CM", "status": "READY", "data": {"cm": {"id": "cert"}}},
        "options": {
            "disableCache": {"enabled": False, "value": False},
            "edgeCacheSettings": {"enabled": True, "value": "345600s"},
            "browserCacheSettings": {"enabled": True, "value": "345600s"},
            "ignoreCookie": {"enabled": True, "value": True},
            "queryParamsOptions": {
                "ignoreQueryString": {"enabled": True, "value": True}
            },
            "allowedHttpMethods": {"enabled": True, "value": ["GET", "HEAD"]},
            "customServerName": {"enabled": True, "value": "read-only.example"},
        },
    }
    client = YandexClient("", required=False)
    try:
        await client.recreate_resource(target, snapshot, "logical-key")
    finally:
        await client.close()
    body = json.loads(route.calls[0].request.content)
    assert body["origin"] == {"originGroupId": "99"}
    assert body["originProtocol"] == "HTTPS"
    assert body["sslCertificate"] == {"type": "CM", "data": {"cm": {"id": "cert"}}}
    assert body["options"]["disableCache"]["value"] is True
    assert body["options"]["browserCacheSettings"]["value"] == "0"
    assert body["options"]["ignoreCookie"]["value"] is False
    assert body["options"]["queryParamsOptions"]["ignoreQueryString"]["value"] is False
    assert body["options"]["allowedHttpMethods"]["value"] == ["GET", "HEAD", "OPTIONS"]
    assert "customServerName" not in body["options"]


def test_yandex_idempotency_key_preserves_existing_uuid():
    key = "87bd3ac8-35f9-40e9-967a-8d8161ce0683"
    assert YandexClient._idempotency_key(key) == key


@pytest.mark.asyncio
@respx.mock
async def test_authorized_key_is_exchanged_and_cached(tmp_path, monkeypatch):
    key_file = tmp_path / "key.json"
    key_file.write_text(
        '{"id":"key-id","service_account_id":"sa-id","private_key":"secret"}', encoding="utf-8"
    )
    monkeypatch.setattr("cdn_controller.clients.jwt.encode", lambda *args, **kwargs: "signed-jwt")
    route = respx.post("https://iam.api.cloud.yandex.net/iam/v1/tokens").mock(
        return_value=Response(200, json={"iamToken": "iam-token", "expiresAt": "2099-01-01T00:00:00Z"})
    )
    auth = YandexIamAuth(str(key_file))
    try:
        assert await auth.headers() == {"Authorization": "Bearer iam-token"}
        assert await auth.headers() == {"Authorization": "Bearer iam-token"}
    finally:
        await auth.close()
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_remnawave_274_uses_collection_patch_with_minimal_payload():
    get_route = respx.get("https://panel.example.com/api/hosts/host-uuid").mock(
        return_value=Response(200, json={"response": {
            "uuid": "host-uuid", "address": "old.example.com", "sni": "old.example.com",
            "host": "old.example.com", "remark": "must-not-be-sent",
        }})
    )
    patch_route = respx.patch("https://panel.example.com/api/hosts").mock(
        return_value=Response(200, json={"response": {
            "uuid": "host-uuid", "address": "new.example.com", "sni": "new.example.com",
            "host": "new.example.com",
        }})
    )
    client = RemnawaveClient(
        "https://panel.example.com", "token", "/api/hosts/{host_id}", "/api/hosts"
    )
    try:
        current = await client.get_host("host-uuid")
        await client.patch_host(
            "host-uuid", current, "new.example.com", ["address", "sni", "host"]
        )
    finally:
        await client.close()
    assert get_route.called
    assert patch_route.calls[0].request.content == (
        b'{"uuid":"host-uuid","address":"new.example.com",'
        b'"sni":"new.example.com","host":"new.example.com"}'
    )


@pytest.mark.asyncio
@respx.mock
async def test_cloudflare_validation_uses_dns_scope_not_zone_read():
    route = respx.get("https://api.cloudflare.com/client/v4/zones/zone-id/dns_records").mock(
        return_value=Response(200, json={"success": True, "result": []})
    )
    client = CloudflareClient("token")
    try:
        await client.validate_dns_access("zone-id")
    finally:
        await client.close()
    assert route.calls[0].request.url.params["per_page"] == "1"
