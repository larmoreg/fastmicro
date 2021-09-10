import asyncio
from importlib import __import__
from importlib.util import find_spec
import logging
import logging.config
import pytest
from typing import AsyncGenerator, cast, Type

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import T, MessageABC, MessagingABC
from fastmicro.serializer import SerializerABC
from fastmicro.service import Service
from fastmicro.topic import Topic

backends = ["fastmicro.messaging.memory"]
if find_spec("aiokafka"):
    backends.append("fastmicro.messaging.kafka")
# FIXME: nats and pulsar are broken
"""
if find_spec("nats") and find_spec("stan"):
    backends.append("fastmicro.messaging.nats")
if find_spec("pulsar"):
    backends.append("fastmicro.messaging.pulsar")
"""
if find_spec("aioredis"):
    backends.append("fastmicro.messaging.redis")

serializers = ["fastmicro.serializer.json"]
if find_spec("cbor2"):
    serializers.append("fastmicro.serializer.cbor")
if find_spec("msgpack"):
    serializers.append("fastmicro.serializer.msgpack")

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


@pytest.fixture(params=backends)
def backend(request) -> str:  # type: ignore
    return cast(str, request.param)


@pytest.fixture
def message_type(backend: str) -> Type[MessageABC]:
    return __import__(backend, fromlist=("Message",)).Message  # type: ignore


@pytest.fixture
def messaging_type(backend: str) -> Type[MessagingABC]:
    return __import__(backend, fromlist=("Messaging",)).Messaging  # type: ignore


@pytest.fixture(params=serializers)
def serializer(request) -> str:  # type: ignore
    return __import__(cast(str, request.param), fromlist=("Serializer",)).Serializer  # type: ignore


class UserABC(MessageABC):
    name: str
    delay: int = 0


@pytest.fixture
def user(message_type: Type[MessageABC]) -> Type[UserABC]:
    class User(UserABC, message_type):  # type: ignore
        pass

    return User


class GreetingABC(MessageABC):
    name: str
    greeting: str


@pytest.fixture
def greeting(message_type: Type[MessageABC]) -> Type[GreetingABC]:
    class Greeting(GreetingABC, message_type):  # type: ignore
        pass

    return Greeting


@pytest.fixture
async def messaging(
    messaging_type: Type[MessagingABC],
    event_loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[MessagingABC[T], None]:
    _messaging: MessagingABC[T] = messaging_type(loop=event_loop)
    await _messaging.connect()
    yield _messaging
    await _messaging.cleanup()


@pytest.fixture
def service(
    messaging: MessagingABC[T], event_loop: asyncio.AbstractEventLoop
) -> Service:
    return Service("test", messaging, loop=event_loop)


@pytest.fixture
def user_topic(user: Type[UserABC], serializer: Type[SerializerABC]) -> Topic[UserABC]:
    return Topic("user", user, serializer=serializer)


@pytest.fixture
def greeting_topic(
    greeting: Type[GreetingABC], serializer: Type[SerializerABC]
) -> Topic[GreetingABC]:
    return Topic("greeting", greeting, serializer=serializer)


@pytest.fixture
def entrypoint(
    service: Service,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
) -> Entrypoint[UserABC, GreetingABC]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(message: UserABC) -> GreetingABC:
        logger.debug(f"Sleeping for {message.delay}s")
        await asyncio.sleep(message.delay)
        logger.debug(f"Waking up after {message.delay}s")
        return greeting(name=message.name, greeting=f"Hello, {message.name}!")

    return greet


@pytest.fixture
def invalid(
    service: Service,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
) -> Entrypoint[UserABC, GreetingABC]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(message: UserABC) -> GreetingABC:
        raise RuntimeError("Test")

    return greet
