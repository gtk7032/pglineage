from __future__ import annotations

from typing import Dict, List, Union


class ParsedStatement:
    def __init__(self, layer, columns, tables, refcolumns, reftables) -> None:
        from field import Field
        from restarget import ResTarget
        from table import Table

        self.layer: int = layer
        self.columns: List[ResTarget] = columns
        self.tables: List[Table] = tables
        self.refcolumns: Dict[str, List[Field]] = refcolumns
        self.reftables: Dict[str, Union[str, ParsedStatement]] = reftables

    def __str__(self) -> str:
        return (
            (
                f"layer: {self.layer}\n"
                + "tables: \n\t"
                + "\n\t".join([str(t) for i, t in enumerate(self.tables)])
                + "\n"
                + "ref-columns: \n\t"
                + "\n\t".join(
                    [
                        "column " + str(i) + ": " + ", ".join([str(f) for f in v])
                        for i, v in enumerate(self.refcolumns)
                    ]
                )
                + "\n"
                + "ref-tables: \n\t"
                + "\n\t".join(
                    [
                        k + ": " + (v if isinstance(v, str) else "")
                        for k, v in self.reftables.items()
                    ]
                )
            )
            + "\n\n"
            + "\n".join([k + "→ \n" + str(v) for k, v in self.reftables.items()])
        )
