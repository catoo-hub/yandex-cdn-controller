from cdn_controller.traffic import integrate_rate_series, normalize_timestamp


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


def test_normalizes_millisecond_checkpoint_and_samples():
    total, checkpoint = integrate_rate_series(
        [1_786_811_880_000, 1_786_811_890_000], [100, 100],
        after=1_786_811_880_000,
    )
    assert total == 1000
    assert checkpoint == 1_786_811_890


def test_normalizes_common_unix_timestamp_precisions():
    assert normalize_timestamp(1_786_811_880) == 1_786_811_880
    assert normalize_timestamp(1_786_811_880_000) == 1_786_811_880
    assert normalize_timestamp(1_786_811_880_000_000) == 1_786_811_880
    assert normalize_timestamp(1_786_811_880_000_000_000) == 1_786_811_880
