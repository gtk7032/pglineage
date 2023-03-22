from __future__ import annotations

from typing import Any, Dict, List, Union


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

    def format(self) -> Dict[str, Any]:
        return {
            "layer": self.layer,
            "tables": [str(t) for t in self.tables],
            "ref-columns": [[str(v) for v in rc] for rc in self.refcolumns],
            "ref-tables": {
                k: v if isinstance(v, str) else v.format()
                for k, v in self.reftables.items()
            },
        }
