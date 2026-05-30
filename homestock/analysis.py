from __future__ import annotations

from collections.abc import Iterable
from math import isfinite
from typing import Any

from homestock.models import DailyPrice


def build_technical_indicators(
    prices: Iterable[DailyPrice],
    *,
    sma_period: int = 20,
    ema_period: int = 20,
    rsi_period: int = 14,
    bollinger_period: int = 20,
    bollinger_std_dev: float = 2.0,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
    atr_period: int = 14,
    adx_period: int = 14,
    mfi_period: int = 14,
    obv_sma_period: int = 20,
    sma_periods: tuple[int, ...] = (5, 20, 60, 120),
    ema_periods: tuple[int, ...] = (5, 20, 60, 120),
    volume_ma_periods: tuple[int, ...] = (5, 20, 60),
) -> list[dict[str, float | int | str | None]]:
    ordered = sorted(prices, key=lambda item: item.date)
    closes = [float(item.close) for item in ordered]
    highs = [float(item.high) for item in ordered]
    lows = [float(item.low) for item in ordered]
    volumes = [float(item.volume) for item in ordered]

    resolved_sma_periods = _period_set(sma_periods, sma_period)
    resolved_ema_periods = _period_set(ema_periods, ema_period)
    resolved_volume_ma_periods = _period_set(volume_ma_periods, obv_sma_period)

    sma_values_by_period = {period: _simple_moving_average(closes, period) for period in resolved_sma_periods}
    ema_values_by_period = {period: _exponential_moving_average(closes, period) for period in resolved_ema_periods}
    volume_ma_values_by_period = {
        period: _simple_moving_average(volumes, period) for period in resolved_volume_ma_periods
    }
    primary_sma_values = sma_values_by_period.get(sma_period, [None] * len(ordered))
    primary_ema_values = ema_values_by_period.get(ema_period, [None] * len(ordered))
    rsi_values = _relative_strength_index(closes, rsi_period)
    bollinger_middle = _simple_moving_average(closes, bollinger_period)
    bollinger_upper, bollinger_lower = _bollinger_bands(closes, bollinger_period, bollinger_std_dev)
    macd_line, signal_line, histogram = _macd(closes, macd_fast, macd_slow, macd_signal)
    ichimoku_conversion, ichimoku_base, ichimoku_leading_a, ichimoku_leading_b, ichimoku_lagging = _ichimoku(
        highs,
        lows,
        closes,
    )
    atr_values = _average_true_range(highs, lows, closes, atr_period)
    adx_values, plus_di_values, minus_di_values = _directional_movement_index(highs, lows, closes, adx_period)
    obv_values = _on_balance_volume(closes, volumes)
    obv_sma_values = _simple_moving_average(obv_values, obv_sma_period)
    mfi_values = _money_flow_index(highs, lows, closes, volumes, mfi_period)
    chandelier_exit_values = _chandelier_exit_long(highs, atr_values, atr_period)

    rows: list[dict[str, float | int | str | None]] = []
    for index, price in enumerate(ordered):
        cloud_top, cloud_bottom, cloud_bias = _ichimoku_cloud(
            ichimoku_leading_a[index],
            ichimoku_leading_b[index],
        )
        trend_regime = _trend_regime(adx_values[index])
        row: dict[str, float | int | str | None] = {
            "date": price.date,
            "close": price.close,
            "volume": price.volume,
            "sma": _round_or_none(primary_sma_values[index]),
            "ema": _round_or_none(primary_ema_values[index]),
            "rsi": _round_or_none(rsi_values[index]),
            "macd": _round_or_none(macd_line[index]),
            "macd_signal": _round_or_none(signal_line[index]),
            "macd_histogram": _round_or_none(histogram[index]),
            "bollinger_mid": _round_or_none(bollinger_middle[index]),
            "bollinger_upper": _round_or_none(bollinger_upper[index]),
            "bollinger_lower": _round_or_none(bollinger_lower[index]),
            "ichimoku_conversion": _round_or_none(ichimoku_conversion[index]),
            "ichimoku_base": _round_or_none(ichimoku_base[index]),
            "ichimoku_leading_span_a": _round_or_none(ichimoku_leading_a[index]),
            "ichimoku_leading_span_b": _round_or_none(ichimoku_leading_b[index]),
            "ichimoku_lagging_span": _round_or_none(ichimoku_lagging[index]),
            "ichimoku_cloud_top": _round_or_none(cloud_top),
            "ichimoku_cloud_bottom": _round_or_none(cloud_bottom),
            "ichimoku_cloud_bias": cloud_bias,
            "atr": _round_or_none(atr_values[index]),
            "adx": _round_or_none(adx_values[index]),
            "plus_di": _round_or_none(plus_di_values[index]),
            "minus_di": _round_or_none(minus_di_values[index]),
            "trend_regime": trend_regime,
            "obv": _round_or_none(obv_values[index]),
            "obv_sma": _round_or_none(obv_sma_values[index]),
            "mfi": _round_or_none(mfi_values[index]),
            "chandelier_exit_long": _round_or_none(chandelier_exit_values[index]),
        }
        for period in resolved_sma_periods:
            row[f"sma{period}"] = _round_or_none(sma_values_by_period[period][index])
        for period in resolved_ema_periods:
            row[f"ema{period}"] = _round_or_none(ema_values_by_period[period][index])
        for period in resolved_volume_ma_periods:
            volume_ma = volume_ma_values_by_period[period][index]
            row[f"volume_ma{period}"] = _round_or_none(volume_ma)
            row[f"volume_ratio{period}"] = _round_or_none(_ratio_or_none(float(price.volume), volume_ma))
        rows.append(row)
    rows.reverse()
    return rows


def detect_chart_patterns(
    prices: Iterable[DailyPrice],
    *,
    lookback_days: int = 120,
) -> list[dict[str, Any]]:
    ordered = sorted(prices, key=lambda item: item.date)
    if lookback_days > 0:
        ordered = ordered[-lookback_days:]
    if len(ordered) < 5:
        return []

    candidates: list[dict[str, Any]] = []
    _add_trend_structure_candidate(candidates, ordered)
    _add_consolidation_candidate(candidates, ordered)
    _add_breakout_candidate(candidates, ordered)
    _add_triangle_candidate(candidates, ordered)
    _add_double_extreme_candidates(candidates, ordered)
    return sorted(candidates, key=lambda item: float(item.get("confidence") or 0), reverse=True)


def _simple_moving_average(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if period <= 0:
        return result
    rolling_sum = 0.0
    for index, value in enumerate(values):
        rolling_sum += value
        if index >= period:
            rolling_sum -= values[index - period]
        if index >= period - 1:
            result[index] = rolling_sum / period
    return result


def _period_set(periods: tuple[int, ...], *required: int) -> tuple[int, ...]:
    values = {period for period in periods if period > 0}
    values.update(period for period in required if period > 0)
    return tuple(sorted(values))


def _exponential_moving_average(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if not values or period <= 0 or len(values) < period:
        return result
    multiplier = 2.0 / (period + 1)
    seed = sum(values[:period]) / period
    result[period - 1] = seed
    previous = seed
    for index in range(period, len(values)):
        previous = ((values[index] - previous) * multiplier) + previous
        result[index] = previous
    return result


def _relative_strength_index(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if len(values) <= period or period <= 0:
        return result

    gains: list[float] = []
    losses: list[float] = []
    for index in range(1, period + 1):
        change = values[index] - values[index - 1]
        gains.append(max(change, 0.0))
        losses.append(abs(min(change, 0.0)))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    result[period] = _rsi_value(avg_gain, avg_loss)

    for index in range(period + 1, len(values)):
        change = values[index] - values[index - 1]
        gain = max(change, 0.0)
        loss = abs(min(change, 0.0))
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        result[index] = _rsi_value(avg_gain, avg_loss)
    return result


def _rsi_value(avg_gain: float, avg_loss: float) -> float:
    if avg_gain == 0 and avg_loss == 0:
        return 50.0
    if avg_loss == 0:
        return 100.0
    relative_strength = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + relative_strength))


def _bollinger_bands(values: list[float], period: int, std_dev_multiplier: float) -> tuple[list[float | None], list[float | None]]:
    upper: list[float | None] = [None] * len(values)
    lower: list[float | None] = [None] * len(values)
    if period <= 0:
        return upper, lower
    for index in range(period - 1, len(values)):
        window = values[index - period + 1 : index + 1]
        mean = sum(window) / period
        variance = sum((value - mean) ** 2 for value in window) / period
        deviation = variance**0.5
        upper[index] = mean + (deviation * std_dev_multiplier)
        lower[index] = mean - (deviation * std_dev_multiplier)
    return upper, lower


def _macd(values: list[float], fast_period: int, slow_period: int, signal_period: int) -> tuple[list[float | None], list[float | None], list[float | None]]:
    fast_ema = _exponential_moving_average(values, fast_period)
    slow_ema = _exponential_moving_average(values, slow_period)
    macd_line: list[float | None] = [None] * len(values)
    compact_macd: list[float] = []
    compact_indices: list[int] = []
    for index, (fast_value, slow_value) in enumerate(zip(fast_ema, slow_ema)):
        if fast_value is None or slow_value is None:
            continue
        macd_value = fast_value - slow_value
        macd_line[index] = macd_value
        compact_macd.append(macd_value)
        compact_indices.append(index)

    signal_compact = _exponential_moving_average(compact_macd, signal_period)
    signal_line: list[float | None] = [None] * len(values)
    histogram: list[float | None] = [None] * len(values)
    for compact_index, signal_value in enumerate(signal_compact):
        if signal_value is None:
            continue
        actual_index = compact_indices[compact_index]
        signal_line[actual_index] = signal_value
        histogram[actual_index] = macd_line[actual_index] - signal_value if macd_line[actual_index] is not None else None
    return macd_line, signal_line, histogram


def _true_range(highs: list[float], lows: list[float], closes: list[float]) -> list[float]:
    result: list[float] = [0.0] * len(highs)
    if not highs:
        return result
    result[0] = highs[0] - lows[0]
    for index in range(1, len(highs)):
        high_low = highs[index] - lows[index]
        high_prev_close = abs(highs[index] - closes[index - 1])
        low_prev_close = abs(lows[index] - closes[index - 1])
        result[index] = max(high_low, high_prev_close, low_prev_close)
    return result


def _average_true_range(highs: list[float], lows: list[float], closes: list[float], period: int) -> list[float | None]:
    true_ranges = _true_range(highs, lows, closes)
    result: list[float | None] = [None] * len(highs)
    if period <= 0 or len(true_ranges) < period:
        return result
    seed = sum(true_ranges[:period]) / period
    result[period - 1] = seed
    previous = seed
    for index in range(period, len(true_ranges)):
        previous = ((previous * (period - 1)) + true_ranges[index]) / period
        result[index] = previous
    return result


def _directional_movement_index(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int,
) -> tuple[list[float | None], list[float | None], list[float | None]]:
    length = len(highs)
    adx: list[float | None] = [None] * length
    plus_di: list[float | None] = [None] * length
    minus_di: list[float | None] = [None] * length
    if period <= 0 or length <= period:
        return adx, plus_di, minus_di

    true_ranges = _true_range(highs, lows, closes)
    plus_dm = [0.0] * length
    minus_dm = [0.0] * length
    for index in range(1, length):
        up_move = highs[index] - highs[index - 1]
        down_move = lows[index - 1] - lows[index]
        plus_dm[index] = up_move if up_move > down_move and up_move > 0 else 0.0
        minus_dm[index] = down_move if down_move > up_move and down_move > 0 else 0.0

    smoothed_tr = sum(true_ranges[1 : period + 1])
    smoothed_plus_dm = sum(plus_dm[1 : period + 1])
    smoothed_minus_dm = sum(minus_dm[1 : period + 1])
    dx_values: list[float | None] = [None] * length

    for index in range(period, length):
        if index > period:
            smoothed_tr = smoothed_tr - (smoothed_tr / period) + true_ranges[index]
            smoothed_plus_dm = smoothed_plus_dm - (smoothed_plus_dm / period) + plus_dm[index]
            smoothed_minus_dm = smoothed_minus_dm - (smoothed_minus_dm / period) + minus_dm[index]

        if smoothed_tr <= 0:
            continue

        plus_value = 100.0 * (smoothed_plus_dm / smoothed_tr)
        minus_value = 100.0 * (smoothed_minus_dm / smoothed_tr)
        plus_di[index] = plus_value
        minus_di[index] = minus_value

        directional_sum = plus_value + minus_value
        if directional_sum == 0:
            dx_values[index] = 0.0
        else:
            dx_values[index] = 100.0 * abs(plus_value - minus_value) / directional_sum

    first_adx_index = (period * 2) - 1
    if first_adx_index >= length:
        return adx, plus_di, minus_di

    seed_values = [value for value in dx_values[period : first_adx_index + 1] if value is not None]
    if len(seed_values) < period:
        return adx, plus_di, minus_di

    previous_adx = sum(seed_values) / period
    adx[first_adx_index] = previous_adx
    for index in range(first_adx_index + 1, length):
        current_dx = dx_values[index]
        if current_dx is None:
            continue
        previous_adx = ((previous_adx * (period - 1)) + current_dx) / period
        adx[index] = previous_adx
    return adx, plus_di, minus_di


def _on_balance_volume(closes: list[float], volumes: list[float]) -> list[float]:
    result: list[float] = [0.0] * len(closes)
    if not closes:
        return result
    for index in range(1, len(closes)):
        if closes[index] > closes[index - 1]:
            result[index] = result[index - 1] + volumes[index]
        elif closes[index] < closes[index - 1]:
            result[index] = result[index - 1] - volumes[index]
        else:
            result[index] = result[index - 1]
    return result


def _money_flow_index(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float],
    period: int,
) -> list[float | None]:
    result: list[float | None] = [None] * len(closes)
    if period <= 0 or len(closes) <= period:
        return result

    typical_prices = [(high + low + close) / 3.0 for high, low, close in zip(highs, lows, closes)]
    positive_flow = [0.0] * len(closes)
    negative_flow = [0.0] * len(closes)
    for index in range(1, len(closes)):
        raw_money_flow = typical_prices[index] * volumes[index]
        if typical_prices[index] > typical_prices[index - 1]:
            positive_flow[index] = raw_money_flow
        elif typical_prices[index] < typical_prices[index - 1]:
            negative_flow[index] = raw_money_flow

    for index in range(period, len(closes)):
        positive_sum = sum(positive_flow[index - period + 1 : index + 1])
        negative_sum = sum(negative_flow[index - period + 1 : index + 1])
        if positive_sum == 0 and negative_sum == 0:
            result[index] = 50.0
            continue
        if negative_sum == 0:
            result[index] = 100.0
            continue
        money_ratio = positive_sum / negative_sum
        result[index] = 100.0 - (100.0 / (1.0 + money_ratio))
    return result


def _chandelier_exit_long(
    highs: list[float],
    atr_values: list[float | None],
    period: int,
) -> list[float | None]:
    result: list[float | None] = [None] * len(highs)
    if period <= 0:
        return result
    for index in range(period - 1, len(highs)):
        atr_value = atr_values[index]
        if atr_value is None:
            continue
        highest_high = max(highs[index - period + 1 : index + 1])
        result[index] = highest_high - (atr_value * 2.0)
    return result


def _ichimoku(
    highs: list[float],
    lows: list[float],
    closes: list[float],
) -> tuple[list[float | None], list[float | None], list[float | None], list[float | None], list[float | None]]:
    conversion = _midpoint_channel(highs, lows, 9)
    base = _midpoint_channel(highs, lows, 26)
    leading_a = _shift_forward(_average_pairs(conversion, base), 26)
    leading_b = _shift_forward(_midpoint_channel(highs, lows, 52), 26)
    lagging = _shift_backward(closes, 26)
    return conversion, base, leading_a, leading_b, lagging


def _midpoint_channel(highs: list[float], lows: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(highs)
    if period <= 0:
        return result
    for index in range(period - 1, len(highs)):
        window_high = max(highs[index - period + 1 : index + 1])
        window_low = min(lows[index - period + 1 : index + 1])
        result[index] = (window_high + window_low) / 2.0
    return result


def _average_pairs(first: list[float | None], second: list[float | None]) -> list[float | None]:
    result: list[float | None] = [None] * len(first)
    for index, (first_value, second_value) in enumerate(zip(first, second)):
        if first_value is None or second_value is None:
            continue
        result[index] = (first_value + second_value) / 2.0
    return result


def _shift_forward(values: list[float | None], periods: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if periods < 0:
        return result
    for index, value in enumerate(values):
        target_index = index + periods
        if value is None or target_index >= len(values):
            continue
        result[target_index] = value
    return result


def _shift_backward(values: list[float], periods: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if periods < 0:
        return result
    for index, value in enumerate(values):
        target_index = index - periods
        if target_index < 0:
            continue
        result[target_index] = value
    return result


def _ichimoku_cloud(
    leading_a: float | None,
    leading_b: float | None,
) -> tuple[float | None, float | None, str | None]:
    if leading_a is None or leading_b is None:
        return None, None, None
    if leading_a > leading_b:
        return leading_a, leading_b, "bullish"
    if leading_a < leading_b:
        return leading_b, leading_a, "bearish"
    return leading_a, leading_b, "flat"


def _trend_regime(adx_value: float | None) -> str | None:
    if adx_value is None:
        return None
    if adx_value > 25.0:
        return "trending"
    if adx_value < 20.0:
        return "ranging"
    return "transitioning"


def _round_or_none(value: float | None) -> float | None:
    if value is None or not isfinite(value):
        return None
    return round(value, 4)


def _ratio_or_none(numerator: float, denominator: float | None) -> float | None:
    if denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _add_trend_structure_candidate(candidates: list[dict[str, Any]], ordered: list[DailyPrice]) -> None:
    window = ordered[-30:] if len(ordered) >= 30 else ordered
    if len(window) < 10:
        return
    split = max(len(window) // 3, 1)
    first = window[:split]
    last = window[-split:]
    first_high = max(item.high for item in first)
    first_low = min(item.low for item in first)
    last_high = max(item.high for item in last)
    last_low = min(item.low for item in last)
    close_change_pct = _percent_change(float(window[0].close), float(window[-1].close))
    if last_high > first_high and last_low > first_low and close_change_pct > 0:
        candidates.append(
            _pattern(
                "uptrend_structure",
                "bullish",
                min(0.85, 0.55 + abs(close_change_pct) / 50.0),
                len(window),
                window[-1].date,
                {"first_low": first_low, "last_low": last_low, "first_high": first_high, "last_high": last_high},
                [
                    "Recent highs and lows are above the early-window highs and lows.",
                    f"Close changed {round(close_change_pct, 4)} percent over the window.",
                ],
            )
        )
    elif last_high < first_high and last_low < first_low and close_change_pct < 0:
        candidates.append(
            _pattern(
                "downtrend_structure",
                "bearish",
                min(0.85, 0.55 + abs(close_change_pct) / 50.0),
                len(window),
                window[-1].date,
                {"first_low": first_low, "last_low": last_low, "first_high": first_high, "last_high": last_high},
                [
                    "Recent highs and lows are below the early-window highs and lows.",
                    f"Close changed {round(close_change_pct, 4)} percent over the window.",
                ],
            )
        )


def _add_consolidation_candidate(candidates: list[dict[str, Any]], ordered: list[DailyPrice]) -> None:
    if len(ordered) < 15:
        return
    window = ordered[-20:] if len(ordered) >= 20 else ordered
    high = max(item.high for item in window)
    low = min(item.low for item in window)
    midpoint = (high + low) / 2.0
    range_pct = ((high - low) / midpoint * 100.0) if midpoint else 0.0
    if range_pct <= 8.0:
        confidence = max(0.45, min(0.78, 0.78 - (range_pct / 30.0)))
        candidates.append(
            _pattern(
                "box_consolidation",
                "neutral",
                confidence,
                len(window),
                window[-1].date,
                {"range_high": high, "range_low": low, "range_pct": round(range_pct, 4)},
                [f"Recent {len(window)} bars stayed inside a {round(range_pct, 4)} percent high-low range."],
            )
        )


def _add_breakout_candidate(candidates: list[dict[str, Any]], ordered: list[DailyPrice]) -> None:
    if len(ordered) < 22:
        return
    latest = ordered[-1]
    previous = ordered[-21:-1]
    previous_high = max(item.high for item in previous)
    previous_low = min(item.low for item in previous)
    avg_volume = sum(item.volume for item in previous) / len(previous)
    volume_ratio = latest.volume / avg_volume if avg_volume else 0.0
    if latest.close > previous_high:
        candidates.append(
            _pattern(
                "range_breakout",
                "bullish",
                min(0.9, 0.58 + max(volume_ratio - 1.0, 0.0) / 3.0),
                21,
                latest.date,
                {
                    "breakout_level": previous_high,
                    "close": latest.close,
                    "prior_20bar_volume_ratio": round(volume_ratio, 4),
                },
                [
                    "Latest close is above the prior 20-bar high.",
                    f"Latest volume is {round(volume_ratio, 4)} times the prior 20-bar average.",
                ],
            )
        )
    elif latest.close < previous_low:
        candidates.append(
            _pattern(
                "range_breakdown",
                "bearish",
                min(0.9, 0.58 + max(volume_ratio - 1.0, 0.0) / 3.0),
                21,
                latest.date,
                {
                    "breakdown_level": previous_low,
                    "close": latest.close,
                    "prior_20bar_volume_ratio": round(volume_ratio, 4),
                },
                [
                    "Latest close is below the prior 20-bar low.",
                    f"Latest volume is {round(volume_ratio, 4)} times the prior 20-bar average.",
                ],
            )
        )


def _add_triangle_candidate(candidates: list[dict[str, Any]], ordered: list[DailyPrice]) -> None:
    if len(ordered) < 24:
        return
    window = ordered[-30:] if len(ordered) >= 30 else ordered
    half = len(window) // 2
    first = window[:half]
    second = window[half:]
    first_high = max(item.high for item in first)
    first_low = min(item.low for item in first)
    second_high = max(item.high for item in second)
    second_low = min(item.low for item in second)
    first_range = first_high - first_low
    second_range = second_high - second_low
    if first_range <= 0:
        return
    if second_high < first_high and second_low > first_low and second_range < first_range * 0.85:
        candidates.append(
            _pattern(
                "symmetrical_triangle_candidate",
                "neutral",
                0.62,
                len(window),
                window[-1].date,
                {
                    "early_high": first_high,
                    "recent_high": second_high,
                    "early_low": first_low,
                    "recent_low": second_low,
                    "range_compression_pct": round((1.0 - (second_range / first_range)) * 100.0, 4),
                },
                ["Recent highs are lower, recent lows are higher, and the range is compressing."],
            )
        )


def _add_double_extreme_candidates(candidates: list[dict[str, Any]], ordered: list[DailyPrice]) -> None:
    if len(ordered) < 25:
        return
    swing_highs = _swing_points(ordered, high=True)
    swing_lows = _swing_points(ordered, high=False)
    top = _matching_swing_pair(swing_highs, tolerance_pct=2.5)
    if top is not None:
        first, second = top
        between = ordered[first["index"] : second["index"] + 1]
        neckline = min(item.low for item in between)
        latest_close = ordered[-1].close
        candidates.append(
            _pattern(
                "double_top_candidate",
                "bearish",
                0.66 if latest_close < neckline else 0.55,
                second["index"] - first["index"] + 1,
                ordered[-1].date,
                {
                    "first_top": first["value"],
                    "second_top": second["value"],
                    "neckline": neckline,
                    "latest_close": latest_close,
                    "status": "confirmed_breakdown" if latest_close < neckline else "candidate",
                },
                ["Two swing highs are close in price after a visible pullback."],
            )
        )
    bottom = _matching_swing_pair(swing_lows, tolerance_pct=2.5)
    if bottom is not None:
        first, second = bottom
        between = ordered[first["index"] : second["index"] + 1]
        neckline = max(item.high for item in between)
        latest_close = ordered[-1].close
        candidates.append(
            _pattern(
                "double_bottom_candidate",
                "bullish",
                0.66 if latest_close > neckline else 0.55,
                second["index"] - first["index"] + 1,
                ordered[-1].date,
                {
                    "first_bottom": first["value"],
                    "second_bottom": second["value"],
                    "neckline": neckline,
                    "latest_close": latest_close,
                    "status": "confirmed_breakout" if latest_close > neckline else "candidate",
                },
                ["Two swing lows are close in price after a visible rebound."],
            )
        )


def _swing_points(ordered: list[DailyPrice], *, high: bool, radius: int = 2) -> list[dict[str, float | int | str]]:
    points: list[dict[str, float | int | str]] = []
    if len(ordered) < (radius * 2) + 1:
        return points
    for index in range(radius, len(ordered) - radius):
        window = ordered[index - radius : index + radius + 1]
        value = float(ordered[index].high if high else ordered[index].low)
        values = [float(item.high if high else item.low) for item in window]
        if high and value == max(values):
            points.append({"index": index, "date": ordered[index].date, "value": value})
        elif not high and value == min(values):
            points.append({"index": index, "date": ordered[index].date, "value": value})
    return points[-8:]


def _matching_swing_pair(
    points: list[dict[str, float | int | str]],
    *,
    tolerance_pct: float,
    min_separation: int = 5,
) -> tuple[dict[str, float | int | str], dict[str, float | int | str]] | None:
    for second_index in range(len(points) - 1, 0, -1):
        second = points[second_index]
        for first_index in range(second_index - 1, -1, -1):
            first = points[first_index]
            if int(second["index"]) - int(first["index"]) < min_separation:
                continue
            reference = max(abs(float(first["value"])), 1.0)
            distance_pct = abs(float(second["value"]) - float(first["value"])) / reference * 100.0
            if distance_pct <= tolerance_pct:
                return first, second
    return None


def _pattern(
    name: str,
    direction: str,
    confidence: float,
    window_days: int,
    observed_at: str,
    levels: dict[str, Any],
    evidence: list[str],
) -> dict[str, Any]:
    return {
        "name": name,
        "direction": direction,
        "confidence": round(confidence, 4),
        "window_days": window_days,
        "observed_at": observed_at,
        "levels": levels,
        "evidence": evidence,
    }


def _percent_change(start: float, end: float) -> float:
    if start == 0:
        return 0.0
    return (end - start) / start * 100.0
