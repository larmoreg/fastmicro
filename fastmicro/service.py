import asyncio
import atexit
from functools import partial
from inflection import underscore
import signal
from typing import Any, Awaitable, Callable, Dict, Optional, Type
import uvloop

from fastmicro.entrypoint import AT, BT, Entrypoint
from fastmicro.messaging import MessagingABC
from fastmicro.messaging.topic import Topic

signal.signal(signal.SIGINT, signal.SIG_DFL)
uvloop.install()


class Service:
    def __init__(
        self,
        name: str,
        messaging: MessagingABC,
        *,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        self.name = name
        self.messaging = messaging
        self.loop = loop
        self.entrypoints: Dict[str, Entrypoint[Any, Any]] = dict()

    def __getattr__(self, name: str) -> Any:
        if name in self.entrypoints:
            return self.entrypoints[name].call
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def entrypoint(
        self,
        schema_type: Type[AT],
        reply_schema_type: Type[BT],
        *args: Any,
        name: Optional[str] = None,
        topic_name: Optional[str] = None,
        reply_topic_name: Optional[str] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs: Any,
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            nonlocal name, topic_name, reply_topic_name, loop
            if not name:
                name = self.name + "_" + underscore(callback.__name__)
            if not topic_name:
                topic_name = name + "_" + underscore(schema_type.__name__)
            if not reply_topic_name:
                reply_topic_name = name + "_" + underscore(reply_schema_type.__name__)

            topics = Topic(
                self.messaging,
                topic_name,
                schema_type,
            )
            reply_topics = Topic(
                self.messaging,
                reply_topic_name,
                reply_schema_type,
            )

            entrypoint = Entrypoint(
                name,
                callback,
                topics,
                reply_topics,
                *args,
                loop=loop if loop is not None else self.loop,
                **kwargs,
            )
            self.entrypoints[callback.__name__] = entrypoint
            return entrypoint

        return _entrypoint

    async def start(self) -> None:
        await self.messaging.connect()
        tasks = [entrypoint.start() for entrypoint in self.entrypoints.values()]
        await asyncio.gather(*tasks)

    def run(self) -> None:
        self.loop.run_until_complete(self.start())
        atexit.register(partial(self.loop.run_until_complete, self.stop))
        self.loop.run_forever()

    async def stop(self) -> None:
        tasks = [entrypoint.stop() for entrypoint in self.entrypoints.values()]
        await asyncio.gather(*tasks)
        await self.messaging.cleanup()
