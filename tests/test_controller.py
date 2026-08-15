import pytest

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
