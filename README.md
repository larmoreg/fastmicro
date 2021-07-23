# FastMicro

<p align="center">
    <em>Fast, simple microservice framework</em>
</p>
<p align="center">
<a href="https://github.com/larmoreg/fastmicro/actions/workflows/main.yml" target="_blank">
    <img src="https://github.com/larmoreg/fastmicro/actions/workflows/main.yml/badge.svg" alt="Test">
</a>
<a href="https://codecov.io/gh/larmoreg/fastmicro" target="_blank">
    <img src="https://codecov.io/gh/larmoreg/fastmicro/branch/master/graph/badge.svg?token=YRMGejrLMC" alt="Coverage">
</a>
<a href="https://pypi.org/project/fastmicro" target="_blank">
    <img src="https://img.shields.io/pypi/v/fastmicro?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
</p>

---

FastMicro is a modern, fast (high-performance) framework for building microservices with Python 3.7+ based on asyncio.

## Install

To install FastMicro run the following:

<div class="termy">

```console
$ pip install fastmicro[redis]
```

</div>

## Example

This example shows how to use the default in-memory backend for evaluation and testing.

**Note**:

The in-memory backend cannot be used for inter-process communication.

### Create it

* Create a file `hello.py` with:

```Python
#!/usr/bin/env python3

import asyncio
from fastmicro.messaging.memory import Message, Messaging
from fastmicro.service import Service
from fastmicro.topic import Topic

messaging: Messaging = Messaging()
service = Service("test", messaging)


class User(Message):
    name: str


class Greeting(Message):
    name: str
    greeting: str


greet_user_topic = Topic("greet_user", User)
greeting_topic = Topic("greeting", Greeting)


@service.entrypoint(greet_user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    greeting = Greeting(name=user.name, greeting=f"Hello, {user.name}!")
    return greeting


async def main() -> None:
    await service.start()

    user = User(name="Greg")
    print(user.dict())
    greeting = await greet.call(user)
    print(greeting.dict())

    await service.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
```

### Run it

```console
$ python hello.py
{'name': 'Greg'}
{'name': 'Greg', 'greeting': 'Hello, Greg!'}
```

## Backends

FastMicro supports the following backends:

* <a href="https://pypi.org/project/aiokafka/" class="external-link" target="_blank">Kafka</a>
* <a href="https://pypi.org/project/aioredis/" class="external-link" target="_blank">Redis</a>
* <a href="https://pypi.org/project/asyncio-nats-streaming/" class="external-link" target="_blank">NATS Streaming</a> (experimental)
* <a href="https://pypi.org/project/pulsar-client/" class="external-link" target="_blank">Apache Pulsar</a> (experimental)

To install FastMicro with one of these backends run one of the following:

<div class="termy">

```console
$ pip install fastmicro[kafka]
$ pip install fastmicro[redis]
$ pip install fastmicro[nats]
$ pip install fastmicro[pulsar]
```

## Another Example

This example shows how to use the Redis backend for inter-process communication.

### Create it

* Create a file `example.py` with:

```Python
#!/usr/bin/env python3

from fastmicro.messaging.redis import Message, Messaging
from fastmicro.service import Service
from fastmicro.topic import Topic

messaging: Messaging = Messaging()
service = Service("test", messaging)


class User(Message):
    name: str


class Greeting(Message):
    name: str
    greeting: str


greet_user_topic = Topic("greet_user", User)
greeting_topic = Topic("greeting", Greeting)


@service.entrypoint(greet_user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    print(user.dict())
    greeting = Greeting(name=user.name, greeting=f"Hello, {user.name}!")
    print(greeting.dict())
    return greeting


if __name__ == "__main__":
    service.run()
```

* Create a file `test.py` with:

```python
#!/usr/bin/env python3

import asyncio
from fastmicro.messaging.redis import Message, Messaging
from fastmicro.service import Service
from fastmicro.topic import Topic

messaging: Messaging = Messaging()
service = Service("test", messaging)


class User(Message):
    name: str


class Greeting(Message):
    name: str
    greeting: str


greet_user_topic = Topic("greet_user", User)
greeting_topic = Topic("greeting", Greeting)


@service.entrypoint(greet_user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    ...


async def main() -> None:
    await messaging.connect()

    user = User(name="Greg")
    print(user.dict())
    greeting = await greet.call(user)
    print(greeting.dict())

    await messaging.cleanup()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
```

### Run it

* In a terminal run:

<div class="termy">

```console
$ python example.py
{'name': 'Greg'}
{'name': 'Greg', 'greeting': 'Hello, Greg!'}
^C
```

* In another terminal run:

<div class="termy">

```console
$ python test.py
{'name': 'Greg'}
{'name': 'Greg', 'greeting': 'Hello, Greg!'}
```

</div>

## License

This project is licensed under the terms of the MIT license.
