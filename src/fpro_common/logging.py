"""统一把日志记录自身的时间显示为北京时间。"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

BEIJING = timezone(timedelta(hours=8), name="Asia/Shanghai")
DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


class BeijingFormatter(logging.Formatter):
    """不依赖操作系统时区，始终输出带 +08:00 的日志时间。"""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        value = datetime.fromtimestamp(record.created, BEIJING)
        if datefmt is not None:
            return value.strftime(datefmt)
        return value.isoformat(timespec="milliseconds")


def configure_beijing_logging(level: int | str = logging.INFO) -> None:
    """为项目自带命令行入口配置北京时间日志。"""
    handler = logging.StreamHandler()
    handler.setFormatter(BeijingFormatter(DEFAULT_LOG_FORMAT))
    logging.basicConfig(level=level, handlers=[handler], force=True)
