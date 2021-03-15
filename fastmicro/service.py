import asyncio
import atexit
import functools
import logging
import signal
from typing import Any, Awaitable, Callable, Dict, Type, Union
import uvloop

from .entrypoint import AT, BT, Entrypoint
from .messaging import Messaging
from .topic import Topic

logger = logging.getLogger(__name__)

signal.signal(signal.SIGINT, signal.SIG_DFL)
uvloop.install()


class Service:
    def __init__(
        self,
        messaging_cls: Union[Type[Messaging], Callable[[], Messaging]],
        name: str,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        self.messaging_cls = functools.partial(messaging_cls, loop=loop)
        self.name = name
        self.loop = loop
        self.entrypoints: Dict[str, Any] = dict()

    def entrypoint(
        self,
        topic: Topic[AT],
        reply_topic: Topic[BT],
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            name = callback.__name__
            if name in self.entrypoints:
                raise ValueError(f"Function {name} already registered in service {self.name}")

            entrypoint = Entrypoint(
                self.messaging_cls,
                self.name + "_" + name,
                callback,
                topic,
                reply_topic,
                loop=self.loop,
            )
            self.entrypoints[name] = entrypoint
            return entrypoint

        return _entrypoint

    def run(self) -> None:
        for entrypoint in self.entrypoints.values():
            self.loop.run_until_complete(entrypoint.run())

        atexit.register(self.kill)
        self.loop.run_forever()

    def kill(self) -> None:
        self.loop.run_until_complete(self.stop())
        self.loop.close()

    async def stop(self) -> None:
        for entrypoint in self.entrypoints.values():
            await entrypoint.stop()
