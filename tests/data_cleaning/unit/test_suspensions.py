from datetime import date

import pytest

from data_cleaning.detector import check_suspend_d

DAY = date(2024, 1, 2)
PARTITION = "trade_date=2024-01-02"


@pytest.mark.parametrize("suspend_type", ["S", "R"])
@pytest.mark.parametrize(
    "timing",
    [
        None,
        "9:30-9:40",
        "09:30-09:40,09:40-09:50",
        "09:40-09:45,09:30-11:00",
        "09:34:21-09:44:21,09:46:15-09:56:15",
        "9:30-9:30:01",
    ],
)
def test_suspension_check_accepts_valid_types_and_all_intervals(
    suspend_type: str, timing: str | None
) -> None:
    row = {
        "ts_code": "000001.SZ",
        "trade_date": DAY,
        "suspend_type": suspend_type,
        "suspend_timing": timing,
    }
    assert check_suspend_d(PARTITION, DAY, [row]) == []


@pytest.mark.parametrize(
    "suspend_type,timing",
    [
        ("?", None),
        (None, "09:30-09:40"),
        ("S", ""),
        ("S", "09:30-09:40,invalid"),
        ("S", "09:30-09:40,"),
        ("S", "25:00-25:10"),
        ("S", "10:00-09:30"),
        ("R", "invalid"),
        ("S", 930),
        ("S", "09:30:60-09:40:00"),
        ("R", "09:30:00-09:40:60"),
        ("S", "09:30:1-09:40:00"),
        ("S", "09:30:00.1-09:40:00"),
    ],
)
def test_suspension_check_reports_bad_type_or_timing(
    suspend_type: str | None, timing: object
) -> None:
    row = {
        "ts_code": "000001.SZ",
        "trade_date": DAY,
        "suspend_type": suspend_type,
        "suspend_timing": timing,
    }
    issues = check_suspend_d(PARTITION, DAY, [row])

    assert len(issues) == 1
    assert issues[0].rule_id == "suspend_value_v1"
    assert issues[0].fix_mode == "MANUAL"
    assert issues[0].observed == {"suspend_type": suspend_type, "suspend_timing": timing}
    assert issues[0].suggested == {
        "action": "REFETCH",
        "start_date": DAY.isoformat(),
        "end_date": DAY.isoformat(),
    }
