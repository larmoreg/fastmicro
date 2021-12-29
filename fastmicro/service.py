import asyncio
import atexit
from functools import partial
import signal
from typing import Any, Awaitable, Callable, Dict
import uvloop

from fastmicro.entrypoint import AT, BT, Entrypoint
from fastmicro.messaging.topic import Topic

signal.signal(signal.SIGINT, signal.SIG_DFL)
uvloop.install()


class Service:
    def __init__(
        self,
        name: str,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        self.name = name
        self.loop = loop
        self.entrypoints: Dict[str, Entrypoint[Any, Any]] = dict()

    def __getattr__(self, name: str) -> Any:
        if name in self.entrypoints:
            return self.entrypoints[name].call
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def entrypoint(
        self, topic: Topic[AT], reply_topic: Topic[BT], **kwargs: Any
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            entrypoint = Entrypoint(
                self.name + "_" + callback.__name__,
                callback,
                topic,
                reply_topic,
                loop=self.loop,
                **kwargs,
            )
            self.entrypoints[callback.__name__] = entrypoint
            return entrypoint

        return _entrypoint

    async def start(self) -> None:
        tasks = [entrypoint.start() for entrypoint in self.entrypoints.values()]
        await asyncio.gather(*tasks)

    def run(self) -> None:
        self.loop.run_until_complete(self.start())
        atexit.register(partial(self.loop.run_until_complete, self.stop))
        self.loop.run_forever()

    async def stop(self) -> None:
        tasks = [entrypoint.stop() for entrypoint in self.entrypoints.values()]
        await asyncio.gather(*tasks)
