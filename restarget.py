from __future__ import annotations

from typing import Any, Dict, List

from field import Field
from table import Table


class ResTarget:
    def __init__(self) -> None:
        self.name = ""
        self.fields: List[Field] = []

    def add_field(self, field: Field) -> None:
        self.fields.append(field)

    def attach_table(self, table: Table) -> None:
        for field in self.fields:
            field.set_table(table)

    def __str__(self) -> str:
        return self.name + " → " + Field.list2str(self.fields)

    @classmethod
    def parse_restarget(cls, tgt: Dict[str, Any], column: ResTarget):
        if "@" not in tgt.keys():
            return

        if tgt["@"] == "ResTarget" and "name" in tgt.keys():
            column.name = tgt["name"]

        elif tgt["@"] == "ColumnRef" and "fields" in tgt.keys():
            field = []
            for elm in tgt["fields"]:
                if "sval" in elm.keys():
                    field.append(elm["sval"])
            if field:
                column.add_field(Field.create_from_list(field))
            return

        for elm in tgt.values():
            if isinstance(elm, dict):
                cls.parse_restarget(elm, column)
