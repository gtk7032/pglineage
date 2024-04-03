import csv
import re
from typing import NamedTuple

from pglineage.stmt import RawStmt


class Row(NamedTuple):
    result: str
    msg: str
    name: str
    stmt: str


class Logger:
    __rows: dict[str, Row] = {}
    __pat = re.compile("[ ]{2,}")

    def set(self, result: str, msg: str, rawstmt: RawStmt) -> None:
        Logger.__rows[rawstmt.name] = Row(result, msg, rawstmt.name, rawstmt.stmt)

    def __sort(self) -> None:
        Logger.__rows = dict(sorted(Logger.__rows.items()))

    def __fmt(self, s: str) -> str:
        res = "".join(s.splitlines())
        res = re.sub(Logger.__pat, " ", res)
        return res[:80] + " ..." if len(res) >= 80 else res[:80]

    def write(self, path: str) -> None:
        self.__sort()
        with open(path, "w", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "status", "error_message", "query"])
            for row in Logger.__rows.values():
                writer.writerow(
                    [row.name, row.result, self.__fmt(row.msg), self.__fmt(row.stmt)]
                )
