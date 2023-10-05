from pathlib import Path

import tqdm

from pglineage.analyzer import Analyzer
from pglineage.reader import FileReader

files = [str(p) for p in Path("resource").glob("**/*") if p.is_file()]
reader = FileReader()
analyzer = Analyzer()

for file in tqdm.tqdm(files, desc="loading", leave=False):
    try:
        sqls = reader.read(file)
    except Exception:
        print("file reading error : " + file)
        continue
    analyzer.load(sqls)

lineage = analyzer.analyze()
lineage.draw(output="output/result", format="jpg")
