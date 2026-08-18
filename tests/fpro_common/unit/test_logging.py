from __future__ import annotations

import logging

from fpro_common import BeijingFormatter


def test_log_record_time_is_always_formatted_as_beijing_time() -> None:
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "message", (), None)
    record.created = 0

    formatted = BeijingFormatter("%(asctime)s %(message)s").format(record)

    assert formatted == "1970-01-01T08:00:00.000+08:00 message"
