import asyncio
from importlib import __import__
from importlib.util import find_spec
import logging
import logging.config
from pydantic import BaseModel
import pytest
from typing import AsyncGenerator, cast, Type

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC
from fastmicro.serializer import SerializerABC
from fastmicro.service import Service
from fastmicro.topic import Topic

backends = ["fastmicro.messaging.memory"]
if find_spec("aiokafka"):
    backends.append("fastmicro.messaging.kafka")
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
def messaging_type(backend: str) -> Type[MessagingABC]:
    return __import__(backend, fromlist=("Messaging",)).Messaging  # type: ignore


@pytest.fixture(params=serializers)
def serializer_type(request) -> Type[SerializerABC]:  # type: ignore
    return __import__(cast(str, request.param), fromlist=("Serializer",)).Serializer  # type: ignore


@pytest.fixture
def messaging(
    messaging_type: Type[MessagingABC],
    event_loop: asyncio.AbstractEventLoop,
) -> MessagingABC:
    return messaging_type(loop=event_loop)


@pytest.fixture
def service(messaging: MessagingABC, event_loop: asyncio.AbstractEventLoop) -> Service:
    return Service("test", messaging, loop=event_loop)


class User(BaseModel):
    name: str


class Greeting(BaseModel):
    name: str
    greeting: str


@pytest.fixture
def user_topic(serializer_type: Type[SerializerABC]) -> Topic[User]:
    return Topic("user", User, serializer_type=serializer_type)


@pytest.fixture
def greeting_topic(serializer_type: Type[SerializerABC]) -> Topic[Greeting]:
    return Topic("greeting", Greeting, serializer_type=serializer_type)


@pytest.fixture
def _entrypoint(
    service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(message: User) -> Greeting:
        return Greeting(name=message.name, greeting=f"Hello, {message.name}!")

    return greet


@pytest.fixture
async def entrypoint(
    service: Service, _entrypoint: Entrypoint[User, Greeting]
) -> AsyncGenerator[Entrypoint[User, Greeting], None]:
    await service.start()
    yield _entrypoint
    await service.stop()


@pytest.fixture
async def invalid(
    service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
) -> AsyncGenerator[Entrypoint[User, Greeting], None]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(message: User) -> Greeting:
        raise RuntimeError("Test")

    await service.start()
    yield greet
    await service.stop()
