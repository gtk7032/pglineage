from __future__ import annotations

from typing import List

from field import Field


class ResTarget:
    def __init__(self) -> None:
        self.name = ""
        self.fields: List[Field] = []

    def add_field(self, field: Field) -> None:
        self.fields.append(field)

    def attach_table(self, table: str) -> None:
        for field in self.fields:
            field.set_table(table)

    def show(self):
        print(f"{self.name=}" + ":" + Field.list_to_str(self.fields))
