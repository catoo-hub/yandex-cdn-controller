import pytest
from pydantic import ValidationError

from cdn_controller.models import AppConfig


def target():
    return {
        "id": "de-main",
        "yandex": {"folder_id": "folder", "origin_group_id": 123, "origin_host_header": "origin.example.com"},
        "domain": {"zone": "example.com", "pattern": "yc-de-{sequence:03d}.example.com", "cloudflare_zone_id": "zone"},
        "remnawave": {"panel": "primary", "host_id": "host"},
        "transport": {"path": "/content/sec.mp4/"},
    }


def test_domain_render_and_defaults():
    cfg = AppConfig.model_validate({
        "providers": {"remnawave": {"primary": {"base_url": "https://panel.example.com", "token_env": "TOKEN"}}},
        "targets": [target()],
    })
    assert cfg.targets[0].domain.render(7) == "yc-de-007.example.com"
    assert cfg.targets[0].rotation.prepare_at_gib == 700


def test_duplicate_target_rejected():
    with pytest.raises(ValidationError):
        AppConfig.model_validate({
            "providers": {"remnawave": {"primary": {"base_url": "https://panel.example.com", "token_env": "TOKEN"}}},
            "targets": [target(), target()],
        })
