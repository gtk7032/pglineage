from __future__ import annotations


class Column:
    def __init__(self, table: str, name: str):
        self.table = table
        self.name = name

    def __str__(self) -> str:
        if self.table:
            return self.table + "." + self.name
        else:
            return self.name

    def set_table(self, table: str) -> None:
        self.table = table

    @staticmethod
    def create_from_list(ls: list[str]):
        name = ls[-1]
        table = ls[-2] if len(ls) == 2 else ""
        return Column(table, name)
