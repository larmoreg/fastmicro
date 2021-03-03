import logging
from typing import Awaitable, Callable, Generic, TypeVar

import pydantic

from .topic import Topic

logger = logging.getLogger(__name__)

AT = TypeVar("AT", bound=pydantic.BaseModel)
BT = TypeVar("BT", bound=pydantic.BaseModel)


class Entrypoint(Generic[AT, BT]):
    def __init__(
        self,
        name: str,
        callback: Callable[[AT], Awaitable[BT]],
        topic: Topic[AT],
        reply_topic: Topic[BT],
    ) -> None:
        self.name = name
        self.callback = callback
        self.topic = topic
        self.reply_topic = reply_topic

    async def process(self) -> None:
        try:
            # TODO: add batch processing?
            async with self.topic.receive(self.name) as input_message:
                output_message = await self.callback(input_message)
                await self.reply_topic.send(output_message)
        except Exception as e:
            logger.exception("Failed to process message; nacking message")
            raise e

    @staticmethod
    def _is_reply(input_message: AT, output_message: BT) -> bool:
        # FIXME: make sure receiving right message
        return True

    async def call(self, input_message: AT) -> BT:
        await self.topic.send(input_message)
        while True:
            async with self.reply_topic.receive(self.name) as output_message:
                if self._is_reply(input_message, output_message):
                    return output_message
