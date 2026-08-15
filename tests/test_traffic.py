from cdn_controller.traffic import integrate_rate_series


def test_integrates_constant_rate():
    total, checkpoint = integrate_rate_series([0, 10, 20], [100, 100, 100])
    assert total == 2000
    assert checkpoint == 20


def test_uses_overlap_without_double_counting():
    total, checkpoint = integrate_rate_series([0, 10, 20, 30], [100, 100, 100, 100], after=20)
    assert total == 1000
    assert checkpoint == 30


def test_empty_series_preserves_checkpoint():
    assert integrate_rate_series([], [], after=123) == (0, 123)

