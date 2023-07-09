-- WITH test1 AS
--     (
--     SELECT customer_id, SUM(amount) AS total_payment FROM payment 
--     GROUP BY customer_id
--     )


-- SELECT test1.customer_id, test1.total_payment, customer.first_name
-- FROM test1 
-- INNER JOIN customer 
-- ON test1.customer_id=customer.customer_id
-- WHERE test1.total_payment>150;

--  SELECT g.id, g.link, g.data, 1
--     FROM graph g
--   UNION ALL
--     SELECT g.id, g.link, g.data, sg.depth + 1
--     FROM graph g, search_graph sg
--     UNION ALL
--  SELECT gx.id, gx.link, gx.data, 1
--     FROM bee gx;

WITH RECURSIVE search_graph(id, link, data, depth) AS (
    SELECT g.id, g.link, g.data, 1
    FROM graph g
  UNION ALL
    SELECT g.id, g2.link, g.data, sg.depth + 1
    FROM graph g, search_graph sg, graph2 g2
    WHERE g.id = sg.link
)
SELECT id, link FROM search_graph;

-- select distinct on (c1) c1, c2 from t1;

-- WITH RECURSIVE r AS (
--     SELECT
--         projects.id as project_id,
--         projects.pre_project_id as pre_project_id,
--         projects.title as title,
--         projects.id as base_project_id -- 集計元となるプロジェクトIDを取得する
--     FROM
--         projects
--     WHERE
--         projects.id <= 3 -- 結果が分かりやすいよう件数を制限
--     UNION ALL
--     SELECT
--         projects.id as project_id,
--         projects.pre_project_id as pre_project_id,
--         projects.title as title,
--         r.base_project_id as base_project_id -- WITH RECURSIVEのデータからデータを引き継ぐ
--     FROM
--         projects,r
--     WHERE
--        projects.id = r.pre_project_id -- ここで再帰的にデータを紐づけ
-- )

-- SELECT
--     project_id
-- FROM
--     r
-- ORDER BY base_project_id ASC;