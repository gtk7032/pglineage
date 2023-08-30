import os
import re
from typing import Any, Tuple

from chardet import detect


class FileReader:
    def __init__(self) -> None:
        self.p1 = re.compile(
            "(?:with|select|update|insert|delete).+?;",
            flags=re.IGNORECASE + re.DOTALL,
        )
        self.p2 = re.compile("--.*")
        self.p3 = re.compile("/\*.*?\*/", flags=re.DOTALL)

    def detect_enc(self, path: str) -> dict[str, Any]:
        with open(path, "rb") as f:
            b = f.read()
            return detect(b)

    def read(self, path: str) -> list[Tuple[str, str]]:
        enc = self.detect_enc(path)
        with open(path, "r", encoding=enc["encoding"]) as f:
            s = f.read().lower()
        s = self.p2.sub("", s)
        s = self.p3.sub("", s)
        sqls = self.p1.findall(s)
        name, _ = os.path.splitext(os.path.basename(path))
        name = name.lower()
        return [(name + "-" + str(i + 1), sql) for i, sql in enumerate(sqls)]
