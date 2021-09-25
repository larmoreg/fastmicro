import asyncio
from importlib import __import__
from importlib.util import find_spec
import logging
import logging.config
from pydantic import BaseModel
import pytest
from typing import AsyncGenerator, cast, Optional, Type

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC, TopicABC
from fastmicro.serializer import SerializerABC
from fastmicro.service import Service

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


@pytest.fixture(scope="session")
def event_loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_event_loop()


@pytest.fixture(params=backends, scope="session")
def backend(request) -> str:  # type: ignore
    return cast(str, request.param)


@pytest.fixture(scope="session")
def messaging_type(backend: str) -> Type[MessagingABC]:
    return __import__(backend, fromlist=("Messaging",)).Messaging  # type: ignore


@pytest.fixture(params=serializers, scope="session")
def serializer_type(request) -> Type[SerializerABC]:  # type: ignore
    return __import__(cast(str, request.param), fromlist=("Serializer",)).Serializer  # type: ignore


@pytest.fixture(scope="session")
async def messaging(
    messaging_type: Type[MessagingABC],
    event_loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[MessagingABC, None]:
    messaging = messaging_type(loop=event_loop)
    async with messaging:
        yield messaging


@pytest.fixture
def service(messaging: MessagingABC, event_loop: asyncio.AbstractEventLoop) -> Service:
    return Service("test", messaging, loop=event_loop)


class User(BaseModel):
    name: str
    delay: Optional[float] = None


class Greeting(BaseModel):
    name: str
    greeting: str


@pytest.fixture
def user_topic(
    backend: str,
    messaging: MessagingABC,
    serializer_type: Type[SerializerABC],
) -> TopicABC[User]:
    topic_type = __import__(backend, fromlist=("Topic",)).Topic  # type: ignore
    return topic_type[User]("user", messaging, serializer_type=serializer_type)  # type: ignore


@pytest.fixture
def greeting_topic(
    backend: str,
    messaging: MessagingABC,
    serializer_type: Type[SerializerABC],
) -> TopicABC[Greeting]:
    topic_type = __import__(backend, fromlist=("Topic",)).Topic  # type: ignore
    return topic_type[Greeting]("greeting", messaging, serializer_type=serializer_type)  # type: ignore


@pytest.fixture
def _entrypoint(
    service: Service,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(message: User) -> Greeting:
        if message.delay:
            await asyncio.sleep(message.delay)
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
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
) -> AsyncGenerator[Entrypoint[User, Greeting], None]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(message: User) -> Greeting:
        raise RuntimeError("Test")

    await service.start()
    yield greet
    await service.stop()
