"""通过 uv 启动时使用的轻量入口。"""

from __future__ import annotations

from qmt_agent.windows_launcher import main

if __name__ == "__main__":
    raise SystemExit(main())
