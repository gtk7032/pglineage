from __future__ import annotations

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

    def format(self) -> dict[str, str | Table]:
        return {"name": self.name, "table": self.table.format()}

    def set_table(self, table: Table) -> None:
        self.table = table

    @staticmethod
    def create_from_list(ls: list[str]):
        name = ls[-1]
        table = ls[-2] if len(ls) == 2 else ""
        return Column(Table("", table), name)
