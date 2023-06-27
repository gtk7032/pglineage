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

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Column):
            return False
        return self.table == __o.table and self.name == __o.name

    def __hash__(self) -> int:
        res = 17
        res = 31 * res + hash(self.table)
        res = 31 * res + hash(self.name)
        return res

    def set_table(self, table: str) -> None:
        if not self.table:
            self.table = table

    def replace_table(self, old: str, new: str) -> None:
        if self.table == old:
            self.table = new

    @staticmethod
    def create_from_list(ls: list[str]):
        name = ls[-1]
        table = ls[-2] if len(ls) >= 2 else ""
        return Column(table, name)
