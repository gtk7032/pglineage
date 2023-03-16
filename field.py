from __future__ import annotations

from typing import List


class Field:
    def __init__(self, table: str, name: str):
        self.table = table
        self.name = name

    def set_table(self, table: str) -> None:
        self.table = table

    def to_str(self) -> str:
        return self.table + "." + self.name

    @staticmethod
    def list_to_str(fields: List[Field]) -> str:
        res = ""
        for field in fields:
            res += " " + field.to_str()
        return res

    def show(self) -> None:
        print(f"{self.table=}, {self.name=}")

    @staticmethod
    def create_from_list(ls: List[str]):
        name = ls[-1]
        table = ls[-2] if len(ls) == 2 else ""
        return Field(table, name)
