from pathlib import Path

from analyzer import Analyzer
from reader import FileReader

files = [str(p) for p in Path("resource").glob("**/*") if p.is_file()]

reader = FileReader()
analyzer = Analyzer()

for file in files:
    analyzer.load(reader.read(file))

lineage = analyzer.analyze()
lineage.draw(output="output/result", format="png")
