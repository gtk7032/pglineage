import os
import re
from typing import Tuple

from chardet import detect


class FileReader:
    def __init__(self) -> None:
        self.p0 = re.compile("('.*?);(.*?')")
        self.p1 = re.compile(
            "(?:with|select|update|insert|delete).+?;",
            flags=re.IGNORECASE + re.DOTALL,
        )
        self.p2 = re.compile("--.*")
        self.p3 = re.compile("/\*.*?\*/", flags=re.DOTALL)

    def detect_enc(self, path: str) -> str | None:
        with open(path, "rb") as f:
            b = f.read()
            e = detect(b)
            return e["encoding"]

    def read(self, path: str) -> list[Tuple[str, str]]:
        enc = self.detect_enc(path)
        if not enc:
            raise Exception()
        with open(path, "r", encoding=enc) as f:
            s = f.read().lower()
        s = self.p0.sub(r"\1\2", s)
        s = self.p2.sub("", s)
        s = self.p3.sub("", s)
        sqls = self.p1.findall(s)
        name, _ = os.path.splitext(os.path.basename(path))
        name = name.lower()
        return [(name + "-" + str(i + 1), sql) for i, sql in enumerate(sqls)]
