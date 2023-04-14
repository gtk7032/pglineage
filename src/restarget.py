from __future__ import annotations

from typing import Any

from column import Column
from table import Table


class ResTarget:
    def __init__(self, alias: str = "", refcols: list[Column] = []) -> None:
        self.alias = alias
        self.refcols = refcols

    def __str__(self) -> str:
        return str(self.format())

    def format(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "refcols": ", ".join([str(rc) for rc in self.refcols]),
        }

    def add_refcol(self, col: Column) -> None:
        self.refcols.append(col)

    def attach_table(self, table: Table) -> None:
        for rc in self.refcols:
            rc.set_table(table)

    @classmethod
    def parse_restarget_list(cls, tgtlst: list[dict[str, Any]]) -> list[ResTarget]:
        psdlst: list[ResTarget] = []

        for tgt in tgtlst:
            if "@" not in tgt.keys() or tgt["@"] != "ResTarget":
                Exception()

            name = tgt["name"] if "name" in tgt.keys() else ""
            refcols: list[Column] = []

            for v in tgt.values():
                if isinstance(v, tuple):
                    for vv in v:
                        cls.extract_refcols(vv, refcols)
                else:
                    cls.extract_refcols(v, refcols)

            psdlst.append(ResTarget(name, refcols))

        return psdlst

    @classmethod
    def extract_refcols(cls, tgt: dict[str, Any], refcols: list[Column]) -> None:
        if not isinstance(tgt, dict):
            return

        if "@" in tgt.keys() and tgt["@"] == "ColumnRef" and "fields" in tgt.keys():
            refcol = []
            for field in tgt["fields"]:
                if "sval" in field.keys():
                    refcol.append(field["sval"])
            if refcol:
                refcols.append(Column.create_from_list(refcol))
            return

        for v in tgt.values():
            if isinstance(v, tuple):
                for vv in v:
                    cls.extract_refcols(vv, refcols)
            else:
                cls.extract_refcols(v, refcols)
