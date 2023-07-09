INSERT INTO table4 (col1, col2) 
SELECT col1, 'c2' FROM table1;

UPDATE table4 SET col2 = table2.col2 
FROM table2, table3 
WHERE table4.col2 = table3.col1;

DELETE FROM table4 
WHERE EXISTS 
( 
  SELECT 1 
  FROM table3 
  WHERE table3.col2 = table4.col2
);

SELECT col1, col2
FROM table4;

