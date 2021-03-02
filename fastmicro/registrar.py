from typing import Dict

from .serializer import Serializer, JsonSerializer


class Registrar:
    def __init__(self) -> None:
        self.registry: Dict[str, Serializer] = dict()

    def register(self, name: str, serializer: Serializer) -> None:
        if name in self.registry:
            raise ValueError(f"Serializer {name} already registered")
        self.registry[name] = serializer

    def get(self, name: str) -> Serializer:
        if name not in self.registry:
            raise ValueError(f"Unknown serializer {name}")
        return self.registry[name]

    def unregister(self, name: str) -> None:
        if name not in self.registry:
            raise ValueError(f"Unknown serializer {name}")
        del self.registry[name]


registrar = Registrar()
json_serializer = JsonSerializer()
registrar.register("json", json_serializer)
