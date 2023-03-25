from __future__ import annotations

from typing import Any, Dict, List

from table import Table


class Column:
    def __init__(self, table: Table, name: str):
        self.table = table
        self.name = name

    def __str__(self) -> str:
        if str(self.table):
            return str(self.table) + "." + self.name
        else:
            return self.name

    def format(self)->Dict[str,str]:
        return {
            "name":self.name,
            "table":self.table
        }

    def set_table(self, table: Table) -> None:
        self.table = table

    @staticmethod
    def create_from_list(ls: List[str]):
        name = ls[-1]
        table = ls[-2] if len(ls) == 2 else ""
        return Column(table, name)
