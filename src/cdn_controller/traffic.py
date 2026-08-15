from __future__ import annotations


def integrate_rate_series(timestamps: list[float], values: list[float], after: float | None = None) -> tuple[float, float | None]:
    """Integrate bytes/second samples using trapezoids, excluding already processed timestamps."""
    all_points = sorted((float(ts), float(value)) for ts, value in zip(timestamps, values) if value is not None)
    if after is None:
        points = all_points
    else:
        anchors = [point for point in all_points if point[0] <= after]
        fresh = [point for point in all_points if point[0] > after]
        points = ([anchors[-1]] if anchors else []) + fresh
    if not points:
        return 0.0, after
    total = 0.0
    for index in range(1, len(points)):
        left, right = points[index - 1], points[index]
        if after is not None and right[0] <= after:
            continue
        delta = max(0.0, right[0] - max(left[0], after or left[0]))
        total += delta * (left[1] + right[1]) / 2.0
    return total, max(after or 0, points[-1][0])


def extract_series(payload: dict) -> tuple[list[float], list[float]]:
    timestamps: list[float] = []
    values: list[float] = []
    for metric in payload.get("metrics", []):
        series = metric.get("timeseries", {})
        ts = series.get("timestamps", [])
        raw = series.get("doubleValues") or series.get("int64Values") or []
        timestamps.extend(float(item) for item in ts)
        values.extend(float(item) for item in raw)
    return timestamps, values
