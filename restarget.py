from __future__ import annotations

from typing import List, Any, Dict

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
        print(f"{self.name=}" + ":" + Field.list2str(self.fields))

    @staticmethod
    def parse_restarget(tgt: Dict[str, Any], column: ResTarget):
        if "@" not in tgt.keys():
            return

        if tgt["@"] == "ColumnRef" and "fields" in tgt.keys():
            field = []
            for elm in tgt["fields"]:
                if "sval" in elm.keys():
                    field.append(elm["sval"])
            if field:
                column.add_field(Field.create_from_list(field))
            return

        for elm in tgt.values():
            if isinstance(elm, dict):
                ResTarget.parse_restarget(elm, column)

