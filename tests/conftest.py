import asyncio
import docker as libdocker
from docker.models.containers import Container
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
async def docker(backend: str) -> Optional[AsyncGenerator[Container, None]]:
    _docker = libdocker.from_env()

    if backend == "fastmicro.messaging.kafka":
        kafka = _docker.containers.run(
            image="aiolibs/kafka:2.12_2.4.0",
            name="test-kafka",
            ports={9092: 9092},
            environment={
                "ADVERTISED_HOST": "localhost",
                "ADVERTISED_PORT": 9092,
            },
            tty=True,
            detach=True,
            remove=True,
        )

        async def _wait_for_kafka(url: str) -> None:
            from aiokafka.client import AIOKafkaClient
            from aiokafka.errors import KafkaConnectionError

            while True:
                try:
                    client = AIOKafkaClient(bootstrap_servers=url)
                    await client.bootstrap()

                    if client.cluster.brokers():
                        return
                except KafkaConnectionError:
                    await asyncio.sleep(0.1)
                    pass
                finally:
                    await client.close()

        try:
            await asyncio.wait_for(_wait_for_kafka("localhost:9092"), timeout=60)
        except asyncio.TimeoutError:
            pytest.exit("Could not start Kafka server")

        yield kafka

        kafka.remove(force=True)
    elif backend == "fastmicro.messaging.redis":
        redis = _docker.containers.run(
            image="redis:6.2.5",
            name="test-redis",
            ports={6379: 6379},
            tty=True,
            detach=True,
            remove=True,
        )

        async def _wait_for_redis(url: str) -> None:
            import aioredis
            from aioredis.exceptions import ConnectionError

            while True:
                try:
                    redis = aioredis.from_url(url)  # type: ignore

                    await redis.set("foo", "bar")
                    assert await redis.get("foo") == b"bar"
                    return
                except ConnectionError:
                    await asyncio.sleep(0.1)
                    pass

        try:
            await asyncio.wait_for(
                _wait_for_redis("redis://localhost:6379"), timeout=60
            )
        except asyncio.TimeoutError:
            pytest.exit("Could not start Redis server")

        yield redis

        redis.remove(force=True)
    else:
        yield None

    _docker.close()


@pytest.fixture(scope="session")
def messaging_type(backend: str) -> Type[MessagingABC]:
    return __import__(backend, fromlist=("Messaging",)).Messaging  # type: ignore


@pytest.fixture(params=serializers, scope="session")
def serializer_type(request) -> Type[SerializerABC]:  # type: ignore
    return __import__(cast(str, request.param), fromlist=("Serializer",)).Serializer  # type: ignore


@pytest.fixture(scope="session")
async def messaging(
    docker: Optional[Container],
    messaging_type: Type[MessagingABC],
    event_loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[MessagingABC, None]:
    messaging = messaging_type(loop=event_loop)
    async with messaging:
        yield messaging


@pytest.fixture
def service(
    messaging: MessagingABC,
    event_loop: asyncio.AbstractEventLoop,
    caplog: pytest.LogCaptureFixture,
) -> Service:
    caplog.set_level(logging.CRITICAL, logger="aiokafka.consumer.group_coordinator")

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
) -> TopicABC[User]:
    topic_type = __import__(backend, fromlist=("Topic",)).Topic  # type: ignore
    return topic_type("user", messaging, User)  # type: ignore


@pytest.fixture
def greeting_topic(
    backend: str,
    messaging: MessagingABC,
) -> TopicABC[Greeting]:
    topic_type = __import__(backend, fromlist=("Topic",)).Topic  # type: ignore
    return topic_type("greeting", messaging, Greeting)  # type: ignore


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
def _invalid(
    service: Service,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(message: User) -> Greeting:
        raise RuntimeError("Test")

    return greet


@pytest.fixture
async def invalid(
    service: Service, _invalid: Entrypoint[User, Greeting]
) -> AsyncGenerator[Entrypoint[User, Greeting], None]:
    await service.start()
    yield _invalid
    await service.stop()
