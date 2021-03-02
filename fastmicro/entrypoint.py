import logging
import pydantic
from typing import Awaitable, Callable, Generic, TypeVar, TYPE_CHECKING

from .messaging import Messaging
from .topic import Topic

if TYPE_CHECKING:
    from .service import Service

logger = logging.getLogger(__name__)

AT = TypeVar("AT", bound=pydantic.BaseModel)
BT = TypeVar("BT", bound=pydantic.BaseModel)


class Entrypoint(Generic[AT, BT]):
    def __init__(
        self,
        service: "Service",
        callback: Callable[[AT], Awaitable[BT]],
        name: str,
        messaging: Messaging,
        topic: Topic[AT],
        reply_topic: Topic[BT],
        mock: bool = False,
    ) -> None:
        self.service = service
        self.callback = callback
        self.name = name
        self.messaging = messaging
        self.topic = topic
        self.reply_topic = reply_topic
        self.mock = mock

    async def process(self) -> None:
        try:
            # TODO: add batch processing?
            input_message = await self.service.receive(self.topic)
            output_message = await self.callback(input_message)
            await self.service.send(self.reply_topic, output_message)

            await self.messaging.ack(self.topic.name, self.name)
        except Exception as e:
            logger.exception("Failed to process message; nacking message")
            await self.messaging.nack(self.topic.name, self.name)
            raise e

    @staticmethod
    def _is_reply(output_message: BT, input_message: AT) -> bool:
        # FIXME: make sure receiving right message
        return True

    async def call(self, input_message: AT) -> BT:
        try:
            await self.service.send(self.topic, input_message)
            if self.mock:
                await self.process()
                output_message = await self.service.receive(self.reply_topic)
            else:
                while True:
                    output_message = await self.service.receive(self.reply_topic)
                    await self.messaging.ack(self.reply_topic.name, self.name)
                    if self._is_reply(output_message, input_message):
                        break

            return output_message
        except Exception as e:
            logger.exception("Failed to call service")
            raise e
