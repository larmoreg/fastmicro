#!/usr/bin/env python3

from pydantic import BaseModel

from fastmicro.messaging.redis import Messaging
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
    print(user)
    greeting = Greeting(name=user.name, greeting=f"Hello, {user.name}!")
    print(greeting)
    return greeting


if __name__ == "__main__":
    service.run()
