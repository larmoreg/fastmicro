from contextlib import asynccontextmanager
import logging
from typing import (
    AsyncIterator,
    Generic,
    Optional,
    Sequence,
    Type,
    TYPE_CHECKING,
)

from fastmicro.env import (
    BATCH_SIZE,
    MESSAGING_TIMEOUT,
)
from fastmicro.messaging.header import T, HeaderABC

if TYPE_CHECKING:
    from fastmicro.messaging import MessagingABC

logger = logging.getLogger(__name__)


class Topic(Generic[T]):
    @property
    def header_type(self) -> Type[HeaderABC[T]]:
        return self.messaging.header_type(self.schema_type)

    def __init__(
        self,
        messaging: "MessagingABC",
        name: str,
        schema_type: Type[T],
    ):
        self.messaging = messaging
        self.name = name
        self.schema_type = schema_type

    async def connect(self) -> None:
        await self.messaging.connect()

    async def cleanup(self) -> None:
        await self.messaging.cleanup()

    async def subscribe(self, group_name: str) -> None:
        await self.messaging.subscribe(self.name, group_name)

    async def unsubscribe(self, group_name: str) -> None:
        await self.messaging.unsubscribe(self.name, group_name)

    @asynccontextmanager
    async def receive(
        self,
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> AsyncIterator[Sequence[HeaderABC[T]]]:
        await self.messaging.subscribe(self.name, group_name)

        headers = await self.messaging.receive(
            self.name,
            group_name,
            consumer_name,
            self.schema_type,
            batch_size,
            timeout,
        )
        if logger.isEnabledFor(logging.DEBUG):
            for header in headers:
                logger.debug(f"Received {header}")

        try:
            yield headers

            if logger.isEnabledFor(logging.DEBUG):
                for header in headers:
                    logger.debug(f"Acking {header}")
            await self.messaging.ack(self.name, group_name, headers)
        except Exception as e:
            if headers:
                if logger.isEnabledFor(logging.DEBUG):
                    for header in headers:
                        logger.debug(f"Nacking {header}")
                await self.messaging.nack(self.name, group_name, headers)
            raise e

    async def send(
        self,
        headers: Sequence[HeaderABC[T]],
    ) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            for header in headers:
                logger.debug(f"Sending {header}")
        await self.messaging.send(self.name, headers)
