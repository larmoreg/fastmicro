import logging
from typing import Awaitable, Callable, cast, Generic, TypeVar

import pydantic

from .messaging import Header
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
            async with self.topic.receive(self.name) as input_header:
                input_message = input_header.message
                assert input_message
                output_message = await self.callback(input_message)
                output_header = Header(parent=input_header.uuid, message=output_message)
                await self.reply_topic.send(output_header)
        except Exception as e:
            logger.exception("Failed to process message; nacking message")
            raise e

    @staticmethod
    def _is_reply(input_header: Header, output_header: Header) -> bool:
        return output_header.parent == input_header.uuid

    async def call(self, input_message: AT) -> BT:
        input_header = await self.topic.send(input_message)
        while True:
            async with self.reply_topic.receive(self.name) as output_header:
                if self._is_reply(input_header, output_header):
                    output_message = output_header.message
                    assert output_message
                    return cast(BT, output_message)
