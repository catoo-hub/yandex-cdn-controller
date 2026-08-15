from cdn_controller.settings import Settings
from cdn_controller.telegram_bot import format_location, format_status


def test_status_format():
    text = format_status({
        "dry_run": True,
        "targets": [{"target_id": "de", "active_generation_id": 1}],
        "generations": [{"id": 1, "state": "ACTIVE", "fqdn": "yc-de-001.example.com", "bytes_sent": 1024 ** 3}],
    })
    assert "yc-de-001.example.com" in text
    assert "1.00 GiB" in text


def test_topic_map_and_location():
    settings = Settings(telegram_topic_map="-100123:42,-100456:7")
    assert settings.topic_map == {-100123: 42, -100456: 7}
    assert format_location(-100123, 42) == "chat_id: -100123\nmessage_thread_id: 42"
