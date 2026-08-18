"""命令行启动入口。"""

from __future__ import annotations

import uvicorn

from fpro_common import configure_beijing_logging
from qmt_agent.api import create_app
from qmt_agent.config import Settings


def main() -> None:
    settings = Settings.from_env()
    configure_beijing_logging(settings.log_level.upper())
    uvicorn.run(
        create_app(settings=settings),
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
        log_config=None,
        workers=1,
    )


if __name__ == "__main__":
    main()
