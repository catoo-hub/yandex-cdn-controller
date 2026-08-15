import pytest
import respx
from httpx import Response

from cdn_controller.clients import YandexClient, YandexIamAuth, webhook_signature
from cdn_controller.models import TargetConfig


def test_webhook_signature_is_stable():
    assert webhook_signature("secret", b"body") == "dc46983557fea127b43af721467eb9b3fde2338fe3e14f51952aa8478c13d355"


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
