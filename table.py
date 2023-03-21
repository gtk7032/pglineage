from __future__ import annotations


class Table:
    def __init__(self, entity: str, alias: str) -> None:
        self.entity = entity
        self.alias = alias

    def __str__(self) -> str:
        return self.alias if self.alias else self.entity
