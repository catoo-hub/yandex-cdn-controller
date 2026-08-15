import pytest
import respx
from httpx import Response

from cdn_controller.clients import YandexClient, webhook_signature
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
    client = YandexClient("key")
    try:
        await client.create_resource(target, "yc-001.example.com", "cert-1", "idem")
    finally:
        await client.close()
    body = route.calls[0].request.content.decode()
    assert '"origin":{"originGroupId":"42"}' in body
    assert '"data":{"cm":{"id":"cert-1"}}' in body
    assert '"disableCache":{"enabled":true,"value":true}' in body
