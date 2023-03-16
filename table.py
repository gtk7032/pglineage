from __future__ import annotations

from typing import List


class Table:
    def __init__(self, name: str, alias: str = "") -> None:
        self.name = name
        self.alias = alias

    def __str__(self) -> str:
        return "(" + self.name + "," + self.alias + ")"

    @staticmethod
    def list2str(tables:List[Table])->str:
        return ", ".join([ str(table) for table in tables])
