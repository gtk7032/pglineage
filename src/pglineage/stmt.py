from dataclasses import dataclass


@dataclass(frozen=True)
class RawStmt:
    name: str
    stmt: str
