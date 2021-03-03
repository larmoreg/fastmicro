import asyncio
from typing import Any, Awaitable, Callable, Dict, List

from .entrypoint import AT, BT, Entrypoint
from .messaging import Messaging
from .topic import Topic


class Service:
    def __init__(
        self,
        name: str,
        messaging: Messaging,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        self.name = name
        self.messaging = messaging
        self.loop = loop
        self.entrypoints: Dict[str, Any] = dict()
        self.tasks: List[asyncio.Task[None]] = list()

    def entrypoint(
        self, topic: Topic[AT], reply_topic: Topic[BT]
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            if topic.name in self.entrypoints:
                raise ValueError(f"Entrypoint already registered for topic {topic.name}")

            entrypoint = Entrypoint(self.name, callback, topic, reply_topic)
            self.entrypoints[topic.name] = entrypoint

            task = self.loop.create_task(self.process(entrypoint), name=self.name)
            self.tasks.append(task)
            return entrypoint

        return _entrypoint

    @staticmethod
    async def process(entrypoint: Entrypoint[AT, BT]) -> None:
        while True:
            await entrypoint.process()
