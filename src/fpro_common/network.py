"""网络连接的进程级公共配置。"""

from __future__ import annotations

import os

PROXY_ENVIRONMENT_VARIABLES = frozenset(
    {
        "all_proxy",
        "ftp_proxy",
        "http_proxy",
        "https_proxy",
        "no_proxy",
    }
)


def disable_environment_proxies() -> None:
    """清除代理地址，并要求支持 NO_PROXY 的客户端全部直连。"""
    for name in tuple(os.environ):
        # casefold 同时覆盖大写、小写及混合大小写的环境变量名。
        if name.casefold() in PROXY_ENVIRONMENT_VARIABLES:
            os.environ.pop(name, None)
    # requests/urllib 在 Windows 上可能从系统设置重新发现代理；全量 bypass
    # 可让支持 NO_PROXY 的 QMT 依赖保持直连。
    os.environ["NO_PROXY"] = "*"
