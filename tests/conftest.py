import asyncio
import logging
import logging.config
import pytest
from typing import AsyncGenerator

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import T, Messaging

# from fastmicro.messaging.kafka import KafkaMessage, KafkaMessaging
from fastmicro.messaging.memory import MemoryMessage, MemoryMessaging

# from fastmicro.messaging.redis import RedisMessage, RedisMessaging
from fastmicro.service import Service
from fastmicro.topic import Topic

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class User(MemoryMessage):
    name: str
    delay: int = 0


class Greeting(MemoryMessage):
    name: str
    greeting: str


@pytest.fixture
async def messaging(
    event_loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[Messaging[T], None]:
    _messaging: Messaging[T] = MemoryMessaging(loop=event_loop)
    await _messaging.connect()
    yield _messaging
    await _messaging.cleanup()


@pytest.fixture
def service(messaging: Messaging[T], event_loop: asyncio.AbstractEventLoop) -> Service:
    return Service(messaging, "test", loop=event_loop)


@pytest.fixture
def user_topic() -> Topic[User]:
    return Topic("user", User)


@pytest.fixture
def greeting_topic() -> Topic[Greeting]:
    return Topic("greeting", Greeting)


@pytest.fixture
def entrypoint(
    service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(message: User) -> Greeting:
        logger.debug(f"Sleeping for {message.delay}s")
        await asyncio.sleep(message.delay)
        logger.debug(f"Waking up after {message.delay}s")
        return Greeting(name=message.name, greeting=f"Hello, {message.name}!")

    return greet


@pytest.fixture
def invalid(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(message: User) -> Greeting:
        raise RuntimeError("Test")

    return greet
