import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from cdn_controller.controller import Controller
from cdn_controller.db import Database
from cdn_controller.models import AppConfig, RotationState
from cdn_controller.settings import Settings


def test_nested_dns_challenge_is_parsed():
    challenge = Controller._dns_challenge({
        "challenges": [{
            "type": "DNS",
            "dnsChallenge": {
                "name": "_acme-challenge.example.com.",
                "type": "CNAME",
                "value": "validation.example.net.",
            },
        }]
    })
    assert challenge == {
        "type": "CNAME",
        "name": "_acme-challenge.example.com.",
        "value": "validation.example.net.",
    }


@pytest.mark.asyncio
async def test_current_yandex_resource_without_status_is_ready():
    resource = {
        "id": "resource-id",
        "active": True,
        "providerCname": "provider.gslb.yccdn.ru",
        "sslCertificate": {"type": "CM", "status": "READY"},
    }
    controller = object.__new__(Controller)
    controller.yandex = SimpleNamespace(get_resource=AsyncMock(return_value=resource))
    assert await controller._wait_resource("resource-id") == resource


@pytest.mark.asyncio
async def test_dry_run_rotation_never_changes_active_generation(tmp_path):
    config = AppConfig.model_validate({
        "providers": {"remnawave": {"primary": {
            "base_url": "https://panel.example.com", "token_env": "REMNAWAVE_TOKEN"
        }}},
        "targets": [{
            "id": "swe-main",
            "yandex": {"folder_id": "folder", "origin_group_id": 42,
                        "origin_host_header": "origin.example.com"},
            "domain": {"zone": "example.com", "pattern": "yc-swe-{sequence:03d}.example.com",
                       "cloudflare_zone_id": "zone"},
            "remnawave": {"panel": "primary", "host_id": "host"},
            "transport": {"path": "/api/uploadFile/"},
        }],
    })
    db = Database(str(tmp_path / "state.db"))
    controller = Controller(config, Settings(dry_run=True, database_path=db.path), db)
    await controller.initialize()
    try:
        active = await db.import_active("swe-main", "existing-resource", "old.example.com")
        reserve = await db.reserve_generation("swe-main", config.target("swe-main").domain.render)
        await db.update_generation(reserve.id, state=RotationState.READY, resource_id="dry-resource")
        result = await controller.rotate("swe-main")
        current, still_reserve = await db.active_and_reserve("swe-main")
        assert result["changed"] is False
        assert current.id == active.id
        assert still_reserve.id == reserve.id
        assert still_reserve.metadata["rotation_simulated"] is True
    finally:
        await controller.close()


@pytest.mark.asyncio
async def test_recreate_in_place_reuses_provider_endpoint_and_resets_counter(tmp_path):
    config = AppConfig.model_validate({
        "targets": [{
            "id": "direct", "yandex": {"folder_id": "folder", "origin_group_id": 42,
            "origin_protocol": "HTTP", "origin_host_header": "203.0.113.10"},
            "transport": {"path": "/api/uploadFile/"},
            "rotation": {"mode": "recreate_in_place", "recreate_at_gib": 740},
        }],
    })
    db = Database(str(tmp_path / "state.db"))
    controller = Controller(config, Settings(dry_run=False, database_path=db.path), db)
    await controller.initialize()
    try:
        original = await db.import_active(
            "direct", "old-resource", "stable.topology.gslb.yccdn.ru", bytes_sent=800 * 1024 ** 3
        )
        controller.yandex.get_resource = AsyncMock(return_value={
            "id": "old-resource", "cname": "internal-resource-name.example",
            "providerCname": "stable.topology.gslb.yccdn.ru",
            "sslCertificate": {"type": "DONT_USE", "status": "READY"},
        })
        controller.yandex.delete_resource = AsyncMock(return_value={"id": "delete-operation"})
        controller.yandex.recreate_resource = AsyncMock(return_value={"id": "create-operation"})
        controller.yandex.wait_operation = AsyncMock(side_effect=[
            {"response": {}}, {"response": {"id": "new-resource"}},
        ])
        controller._wait_resource = AsyncMock(return_value={
            "id": "new-resource", "active": True,
            "providerCname": "stable.topology.gslb.yccdn.ru",
            "sslCertificate": {"type": "DONT_USE", "status": "READY"},
        })
        controller._wait_health = AsyncMock(return_value=SimpleNamespace(healthy=True, error=None))
        controller.notifier.emit = AsyncMock()

        current = await controller.recreate_in_place("direct")

        assert current.id == original.id
        assert current.resource_id == "new-resource"
        assert current.fqdn == "stable.topology.gslb.yccdn.ru"
        assert current.bytes_sent == 0
        assert current.last_metric_ts is None
        assert "recreate" not in current.metadata
        assert current.metadata["recreate_history"][-1]["old_resource_id"] == "old-resource"
        controller.yandex.recreate_resource.assert_awaited_once_with(
            config.target("direct"), controller.yandex.get_resource.return_value,
            "recreate:direct:1:old-resource",
        )
    finally:
        await controller.close()
