from __future__ import annotations

from typing import List


class Column:
    def __init__(self, name: str, table: str) -> None:
        self.name = name
        self.table = table

    def show(self):
        print(f"{self.name=}, {self.table=}")

    @staticmethod
    def show_(col: List[Column]):
        for c in col:
            c.show()

    @staticmethod
    def create_from_list(ls: list[str]):
        name = ls[-1]
        table = ls[-2] if len(ls) == 2 else ""
        return Column(name, table)

    @staticmethod
    def add_table(table: str, columns: List[List[Column]]):
        for col in columns:
            for c in col:
                if not c.table:
                    c.table = table
