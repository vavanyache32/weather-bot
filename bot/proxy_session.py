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
        self._connector: Optional[aiohttp.BaseConnector] = None
        # Pass HTTP proxies to the standard implementation;
        # SOCKS proxies are handled separately in create_session.
        http_proxy = (
            proxy
            if proxy and not proxy.startswith(("socks5://", "socks4://", "socks://"))
            else None
        )
        super().__init__(proxy=http_proxy, **kwargs)

    async def create_session(self) -> aiohttp.ClientSession:
        # aiogram calls create_session() before every request.
        # Re-use the existing open session so we don't leak ClientSession /
        # Connector objects on every tick.
        if self._session is not None and not self._session.closed:
            return self._session

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
            self._connector = connector
            self._session = aiohttp.ClientSession(connector=connector)
            return self._session
        return await super().create_session()

    async def close(self) -> None:
        # Explicitly close the session + connector. aiohttp-socks ProxyConnector
        # sometimes keeps underlying transports alive after ClientSession.close(),
        # so we close it manually to avoid "Unclosed connector" warnings.
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._connector is not None:
            await self._connector.close()
            self._connector = None
        await super().close()
