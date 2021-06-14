import asyncio
import atexit
import logging
import signal
from typing import Awaitable, Callable, Dict, Optional
import uvloop

from .entrypoint import AT, BT, Entrypoint
from .messaging import Messaging
from .topic import Topic

logger = logging.getLogger(__name__)

signal.signal(signal.SIGINT, signal.SIG_DFL)
uvloop.install()


class Service:
    def __init__(
        self, messaging: Messaging, name: str, loop: Optional[asyncio.AbstractEventLoop] = None
    ) -> None:
        self.messaging = messaging
        self.name = name
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        self.entrypoints: Dict[str, Entrypoint] = dict()

    def entrypoint(
        self, topic: Topic[AT], reply_topic: Topic[BT]
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            name = callback.__name__
            if name in self.entrypoints:
                raise ValueError(f"Function {name} already registered in service {self.name}")

            entrypoint = Entrypoint(
                self.messaging,
                self.name + "_" + name,
                callback,
                topic,
                reply_topic,
                loop=self.loop,
            )
            self.entrypoints[name] = entrypoint
            return entrypoint

        return _entrypoint

    async def _start(self) -> None:
        await self.messaging.connect()

        tasks = [entrypoint.start() for entrypoint in self.entrypoints.values()]
        await asyncio.gather(*tasks)

    def run(self) -> None:
        self.loop.run_until_complete(self._start())
        atexit.register(self.stop)
        self.loop.run_forever()

    async def _stop(self) -> None:
        tasks = [entrypoint.stop() for entrypoint in self.entrypoints.values()]
        await asyncio.gather(*tasks)

        await self.messaging.cleanup()

    def stop(self) -> None:
        self.loop.run_until_complete(self._stop())
        self.loop.close()
