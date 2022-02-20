import asyncio
import docker as libdocker
from docker.models.containers import Container
from importlib import __import__
from importlib.util import find_spec
import logging
import logging.config
from pydantic import BaseModel
import pytest
from pytest import FixtureRequest
from typing import AsyncGenerator, cast, Optional, Type

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC
from fastmicro.messaging.topic import Topic
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
def backend(request: FixtureRequest) -> str:
    return request.param  # type: ignore


@pytest.fixture(scope="session")
async def docker(backend: str) -> Optional[AsyncGenerator[Container, None]]:
    client = libdocker.from_env()

    if backend == "fastmicro.messaging.kafka":
        client.images.pull("aiolibs/kafka:2.13_2.8.1")
        kafka = client.containers.run(
            image="aiolibs/kafka:2.13_2.8.1",
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
        client.images.pull("redis:6.2.6")
        redis = client.containers.run(
            image="redis:6.2.6",
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
                    await redis.delete("foo")
                    return
                except ConnectionError:
                    await asyncio.sleep(0.1)
                    pass
                finally:
                    await redis.close()

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

    client.close()


@pytest.fixture(scope="session")
def messaging_type(backend: str) -> Type[MessagingABC]:
    return cast(
        Type[MessagingABC], __import__(backend, fromlist=("Messaging",)).Messaging  # type: ignore
    )


@pytest.fixture(params=serializers, scope="session")
def serializer_type(request: FixtureRequest) -> Type[SerializerABC]:
    return cast(
        Type[MessagingABC],
        __import__(request.param, fromlist=("Serializer",)).Serializer,  # type: ignore
    )


@pytest.fixture(scope="session")
async def messaging(
    docker: Optional[Container],
    messaging_type: Type[MessagingABC],
    event_loop: asyncio.AbstractEventLoop,
) -> MessagingABC:
    return messaging_type(loop=event_loop)


@pytest.fixture
def _service(
    event_loop: asyncio.AbstractEventLoop,
    caplog: pytest.LogCaptureFixture,
) -> Service:
    caplog.set_level(logging.CRITICAL, logger="aiokafka.consumer.group_coordinator")

    return Service("test", loop=event_loop)


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
) -> Topic[User]:
    return Topic(messaging, "user", User)


@pytest.fixture
def greeting_topic(
    backend: str,
    messaging: MessagingABC,
) -> Topic[Greeting]:
    return Topic(messaging, "greeting", Greeting)


@pytest.fixture
def _entrypoint(
    _service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
) -> Entrypoint[User, Greeting]:
    @_service.entrypoint(user_topic, greeting_topic)
    async def greet(message: User) -> Greeting:
        if message.delay:
            await asyncio.sleep(message.delay)
        return Greeting(name=message.name, greeting=f"Hello, {message.name}!")

    return greet


@pytest.fixture
def _error_entrypoint(
    _service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
) -> Entrypoint[User, Greeting]:
    @_service.entrypoint(user_topic, greeting_topic)
    async def greet(message: User) -> Greeting:
        raise RuntimeError("Test")

    return greet


@pytest.fixture
async def service(
    _service: Service,
    _entrypoint: Entrypoint[User, Greeting],
) -> AsyncGenerator[Service, None]:
    await _service.start()
    yield _service
    await _service.stop()


@pytest.fixture
async def error_service(
    _service: Service,
    _error_entrypoint: Entrypoint[User, Greeting],
) -> AsyncGenerator[Service, None]:
    await _service.start()
    yield _service
    await _service.stop()
