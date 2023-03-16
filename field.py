from __future__ import annotations

from typing import List

from table import Table


class Field:
    def __init__(self, table: Table, name: str):
        self.table = table
        self.name = name

    def set_table(self, table: Table) -> None:
        self.table = table

    def __str__(self) -> str:
        return str(self.table) + "." + self.name

    @staticmethod
    def list2str(fields: List[Field]) -> str:
        return ", ".join([str(f) for f in fields])

    @staticmethod
    def create_from_list(ls: List[str]):
        name = ls[-1]
        table = ls[-2] if len(ls) == 2 else ""
        return Field(Table(table), name)
