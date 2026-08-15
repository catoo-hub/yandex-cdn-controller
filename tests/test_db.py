import pytest

from cdn_controller.db import Database
from cdn_controller.models import RotationState


@pytest.mark.asyncio
async def test_generation_lifecycle(tmp_path):
    db = Database(str(tmp_path / "state.db"))
    await db.initialize()
    await db.ensure_target("de")
    generation = await db.reserve_generation("de", lambda seq: f"yc-de-{seq:03d}.example.com")
    assert generation.sequence == 1
    assert generation.state == RotationState.PREPARING
    generation = await db.update_generation(generation.id, state=RotationState.READY, resource_id="r1")
    assert generation.resource_id == "r1"
    await db.activate("de", generation.id, None, 0)
    active, reserve = await db.active_and_reserve("de")
    assert active.state == RotationState.ACTIVE
    assert reserve is None


@pytest.mark.asyncio
async def test_notification_dedup_is_persistent(tmp_path):
    path = str(tmp_path / "state.db")
    db = Database(path)
    await db.initialize()
    assert await db.claim_notification("same-error", 1800) is True
    reopened = Database(path)
    assert await reopened.claim_notification("same-error", 1800) is False
