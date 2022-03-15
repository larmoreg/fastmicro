#!/usr/bin/env python3

import asyncio
from pydantic import BaseModel

from fastmicro.messaging.memory import Messaging
from fastmicro.service import Service


class User(BaseModel):
    name: str


class Greeting(BaseModel):
    name: str
    greeting: str


messaging = Messaging()
service = Service("test", messaging)


@service.entrypoint(User, Greeting)
async def greet(user: User) -> Greeting:
    greeting = Greeting(name=user.name, greeting=f"Hello, {user.name}!")
    return greeting


async def main() -> None:
    await service.start()

    async with messaging:
        user = User(name="Greg")
        print(user)
        greeting = await service.greet(user)
        print(greeting)

    await service.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
