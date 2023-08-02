UPDATE table4 SET col2 = table2.col2 
FROM table2, table3 
WHERE table4.col2 = table3.col1;