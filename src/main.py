from __future__ import annotations

import argparse
from pprint import pprint

from pglast import parse_sql

from analyzer import Analyzer
from lineage import Lineage

# sql = "select a, b, c from tbl;"
# sql = (
#     "insert into to_table as totbl (to_col1, to_col2) "
#     "select from_table.from_col1, from_table.from_col2 from from_table, ref_table where from_table.col1 = ref_table.col1;"
# )

# sql = "SELECT s1.age * s2.age as al, s1.age_count * 5, 5, 'aa' FROM ( SELECT age, COUNT(age) as age_count FROM students as stu GROUP BY age ) as s1, s2;"
# sql = (
#     "UPDATE "
#     + "XXX a "
#     + " SET "
#     + " attr_name = ( "
#     + "    SELECT "
#     + "    B.attr_name "
#     + "    FROM "
#     + "    attribute B, ref "
#     + "    WHERE "
#     + "    A.attr_id = B.attr_id AND a.attr_id = ref.refid "
#     + " ) "
#     + " FROM C "
#     + " WHERE C.id = (SELECT id FROM EFG);"
# )

# sql = (
#     "SELECT id,  unitid "
#     "FROM t1 "
#     "WHERE id = "
#     "("
#     "    SELECT id"
#     "    FROM t2"
#     ");"
# )

sql = "UPDATE animal SET " "gender = tbl.col " "FROM tbl;"

# sql = (
#     "UPDATE human "
#     "SET gender = "
#     "CASE gen.gender "
#     "    WHEN '女' THEN '男' "
#     "    ELSE '女' "
#     "END;"
# )

# sql = "select case gender when gender = 1 then 2 else 3 end from tbl;"
# sql = "select case when ( select gender from reftable where col = 'x')= 1 then 2 else 3 end from tbl;"
# sql = "select case when col = 1 then col else col2 end from tbl;"
sql = (
    "UPDATE human "
    "SET gender = "
    "CASE "
    "    WHEN EXISTS("
    "    SELECT 1 FROM human2 WHERE age = 1"
    ") THEN '男' "
    "    ELSE '女' "
    "END;"
)

# root = parse_sql(sql)
# stmt = root[0].stmt
# pprint(stmt(skip_none=True))
# exit()
# sql = "insert into to_table (to_col1, to_col2)"
# sql = "select 5 * from_table1.from_col1, from_table2.from_col4 from from_table inner join from_table2 on from_table.from_col3 = from_table2.from_col4;"

# sql = (
#     "insert into to_table (to_col1, to_col2)"
#     "select case when true then from_table1.from_col1 when false then from_table2.from_col4 else 5 end from from_table inner join from_table2 on from_table.from_col3 = from_table2.from_col4;"
# )

# sql = "INSERT INTO new_table ( col1, col2, col3 ) WITH tmp_table AS ( SELECT col1 as colX, col2, col3, 5 FROM old_table ) SELECT col1, col2, col3, 4 FROM tmp_table"

# sql = "SELECT tbl1.res AS res2, tbl2.res, tbl3.res FROM tbl1 INNER JOIN tbl2 ON tbl1.col = tbl2.col, tbl3, tbl4 WHERE tbl1.col2 = TBL4.col2;"
# sql += "SELECT b FROM tbl4;"

# sql = (
#     "with get_top5_amount_id as ("
#     "select customer_id as id, "
#     "sum(amount) sum_amount, "
#     "xxx "
#     "from payment "
#     ") "
#     "select email, get_top5_amount_id.xxx "
#     "from customer, get_top5_amount_id, "
#     "(select * from tbl ) as tbl2 "
#     "join get_top5_amount_id "
#     "on customer.customer_id = get_top5_amount_id.id;"
# )
# sql = "UPDATE EMPLOYEES SET SALARY = 8500 WHERE LAST_NAME = 'Keats';"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--type", choices=[1, 2, 3], type=int)
    args = parser.parse_args()

    analyzer = Analyzer()
    analyzer.load(sql, "hello")
    # for nd in nodes:
    #     pprint(nd.format())
    lineage = analyzer.analyze()
    lineage.draw(args.type)
