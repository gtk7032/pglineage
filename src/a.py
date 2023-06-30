import glob
import os
import re
from pathlib import Path

from analyzer import Analyzer

# paths = glob.glob("resource/*")
path = Path("resource")
# print(Path.is_dir(paths))
files = [p for p in path.glob("**/*") if p.is_file()]


p = re.compile(
    "((?:with|select|update|insert|delete).+?;)",
    flags=re.IGNORECASE + re.DOTALL,
)

analyzer = Analyzer()

for file in files:
    f = os.path.basename(file)
    name, ext = os.path.splitext(f)
    with open(file, "r") as _f:
        s = _f.read()

    x = p.findall(
        s,
    )
    # print(x)

    for q in x:
        # print(q)
        try:
            analyzer.load(q, name)
        except:
            # print(file)
            continue


lineage = analyzer.analyze()
lineage.draw()
