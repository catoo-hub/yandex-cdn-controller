from cdn_controller.telegram_bot import format_status


def test_status_format():
    text = format_status({
        "dry_run": True,
        "targets": [{"target_id": "de", "active_generation_id": 1}],
        "generations": [{"id": 1, "state": "ACTIVE", "fqdn": "yc-de-001.example.com", "bytes_sent": 1024 ** 3}],
    })
    assert "yc-de-001.example.com" in text
    assert "1.00 GiB" in text

