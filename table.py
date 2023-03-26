from __future__ import annotations

from typing import Any, Dict


class Table:
    def __init__(self, alias: str, ref) -> None:
        from parsed import ParsedStatement

        self.alias = alias
        self.ref: str | ParsedStatement = ref

    def __str__(self) -> str:
        return self.ref if isinstance(self.ref, str) and self.ref else self.alias

    def format(self) -> Dict[str, Any]:
        return {
            "alias": self.alias,
            "ref": self.ref if isinstance(self.ref, str) else self.ref.format(),
        }
