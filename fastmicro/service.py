import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict

from .entrypoint import AT, BT, Entrypoint
from .messaging import Messaging
from .topic import T, Topic

logger = logging.getLogger(__name__)


class Service:
    def __init__(self, name: str, messaging: Messaging) -> None:
        self.name = name
        self.messaging = messaging
        self.entrypoints: Dict[str, Any] = dict()
        self.logger = logging.getLogger(__name__)

    def entrypoint(
        self, topic: Topic[AT], reply_topic: Topic[BT], mock: bool = False
    ) -> Callable[[Callable[[AT], Awaitable[BT]]], Entrypoint[AT, BT]]:
        def _entrypoint(callback: Callable[[AT], Awaitable[BT]]) -> Entrypoint[AT, BT]:
            if topic.name in self.entrypoints:
                raise ValueError(f"Entrypoint already registered for topic {topic.name}")

            entrypoint = Entrypoint(
                self, callback, self.name, self.messaging, topic, reply_topic, mock=mock
            )
            self.entrypoints[topic.name] = entrypoint
            return entrypoint

        return _entrypoint

    async def receive(self, topic: Topic[T]) -> T:
        serialized = await self.messaging.receive(topic.name, self.name)
        return await topic.deserialize(serialized)

    async def send(self, topic: Topic[T], message: T) -> None:
        serialized = await topic.serialize(message)
        await self.messaging.send(topic.name, serialized)

    async def process(self) -> None:
        # TODO: start a separate task for each entrypoint
        tasks = [entrypoint.process() for entrypoint in self.entrypoints.values()]
        await asyncio.gather(*tasks)
