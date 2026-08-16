"""命令行启动入口。"""

from __future__ import annotations

import uvicorn

from qmt_agent.api import create_app
from qmt_agent.config import Settings


def main() -> None:
    settings = Settings.from_env()
    uvicorn.run(
        create_app(settings=settings),
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
        workers=1,
    )


if __name__ == "__main__":
    main()
