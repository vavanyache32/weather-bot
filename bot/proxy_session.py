"""Custom aiogram session that supports SOCKS proxies via aiohttp-socks."""
from __future__ import annotations

from typing import Optional

import aiohttp
from aiogram.client.session.aiohttp import AiohttpSession


class ProxyAiohttpSession(AiohttpSession):
    """AiohttpSession that supports both HTTP and SOCKS proxies.

    For SOCKS5 (e.g. Tor) aiohttp-socks is required.
    Example: ``socks5://127.0.0.1:9050``
    """

    def __init__(self, proxy: Optional[str] = None, **kwargs) -> None:
        self._proxy_url = proxy
        # Pass HTTP proxies to the standard implementation;
        # SOCKS proxies are handled separately in create_session.
        http_proxy = (
            proxy
            if proxy and not proxy.startswith(("socks5://", "socks4://", "socks://"))
            else None
        )
        super().__init__(proxy=http_proxy, **kwargs)

    async def create_session(self) -> aiohttp.ClientSession:
        if self._proxy_url and self._proxy_url.startswith(
            ("socks5://", "socks4://", "socks://")
        ):
            try:
                from aiohttp_socks import ProxyConnector
            except ImportError as exc:
                raise RuntimeError(
                    "SOCKS proxy requires 'aiohttp-socks'. "
                    "Install it: pip install aiohttp-socks"
                ) from exc
            connector = ProxyConnector.from_url(self._proxy_url)
            self._session = aiohttp.ClientSession(connector=connector)
            return self._session
        return await super().create_session()
