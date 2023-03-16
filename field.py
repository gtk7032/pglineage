from __future__ import annotations

from typing import List


class Field:
    def __init__(self, table: str, name: str):
        self.table = table
        self.name = name

    def set_table(self, table: str) -> None:
        self.table = table

    def __str__(self) -> str:
        return self.table + "." + self.name

    @staticmethod
    def list2str(fields: List[Field]) -> str:
        res = ""
        for field in fields:
            res += " " + str(field)
        return res

    @staticmethod
    def create_from_list(ls: List[str]):
        name = ls[-1]
        table = ls[-2] if len(ls) == 2 else ""
        return Field(table, name)
