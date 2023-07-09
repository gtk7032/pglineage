import csv
from typing import NamedTuple


class Row(NamedTuple):
    name: str
    result: str
    msg: str
    query: str


class Logger:
    rows: dict[str, Row] = {}

    def set(self, name: str, row: Row) -> None:
        Logger.rows[name] = row

    def __sort(self) -> None:
        Logger.rows = dict(sorted(Logger.rows.items()))

    def write(self, path: str) -> None:
        self.__sort()
        with open(path, "w", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "status", "error_message", "query"])
            for r in Logger.rows.values():
                s = "".join(r.query[:80].splitlines())
                s += " ..." if len(r.query) >= 80 else ""
                writer.writerow([r.name, r.result, r.msg, s])
