#SUBSTRING FUCTION
#multi value function
SELECT SUBSTR(T1_TEXT_1, T1_INT_1, T1_INT_2) from T1 order by 1
SELECT SUBSTR(T1_CHAR_1, T1_INT_1, T1_INT_2)  from T1 order by 1
SELECT SUBSTR(T1_TEXT_1, T1_INT_1, T1_INT_2)  from T1 order by 1
SELECT BIT_LENGTH(T1_TEXT_1) from T1 order by 1
SELECT BIT_LENGTH(T1_CHAR_1) from T1 order by 1
#singe value fucntion
SELECT max(BIT_LENGTH(T1_CHAR_1)) from T1
SELECT min(BIT_LENGTH(T1_CHAR_1)) from T1
SELECT sum(BIT_LENGTH(T1_CHAR_1)) from T1
SELECT avg(BIT_LENGTH(T1_CHAR_1)) from T1
SELECT max(BIT_LENGTH(T1_TEXT_1)) from T1
SELECT min(BIT_LENGTH(T1_TEXT_1)) from T1
SELECT sum(BIT_LENGTH(T1_TEXT_1)) from T1
SELECT avg(BIT_LENGTH(T1_TEXT_1)) from T1
#ELT
SELECT ELT(T1_INT_1, T1_TEXT_1, T1_TEXT_2, T1_CHAR_1, T1_TEXT_3, T2_TEXT_1, T2_CHAR_1) from T1 , T2 where T1_INT_1 = T2_INT_1 order by 1 desc
SELECT ELT(4, T1_TEXT_1, T1_TEXT_2, T1_CHAR_1, T1_TEXT_3, T2_TEXT_1, T2_CHAR_1) from T1 , T2 where T1_INT_1 = T2_INT_1 order by 1 desc
SELECT ELT(3, T1_TEXT_1, T1_TEXT_2, T1_CHAR_1, T1_TEXT_3, T2_TEXT_1, T2_CHAR_1) from T1 , T2 where T1_INT_1 = T2_INT_1 order by 1 desc
SELECT ELT(2, T1_TEXT_1, T1_TEXT_2, T1_CHAR_1, T1_TEXT_3, T2_TEXT_1, T2_CHAR_1) from T1 , T2 where T1_INT_1 = T2_INT_1 order by 1 desc
SELECT ELT((T1_INT_1 MOD T1_INT_2), T1_TEXT_1, T1_TEXT_2, T1_CHAR_1, T1_TEXT_3, T1_INT_1) from T1 order by 1 desc
SELECT FIELD(T1_TEXT_1, T1_TEXT_2, T1_TEXT_1, T1_CHAR_1, T1_INT_1) from T1 order by 1 asc
#FIELD
SELECT FIELD(T1_TEXT_1, T1_TEXT_2, T1_CHAR_1, T1_TEXT_3) from T1 order by 1 desc
SELECT FIELD(T1_TEXT_1, T1_TEXT_2, T1_TEXT_1, T1_CHAR_1, T1_TEXT_3) from T1 order by 1 desc
SELECT FIELD(T1_INT_1, T1_INT_2, T1_INT_3) from T1 order by 1 desc
#INSTR todo enable them
SELECT INSTR(T1_TEXT_1, T1_CHAR_1) from T1 order by 1 desc
#LOCATE todo enable them
SELECT LOCATE(T1_CHAR_1, T1_TEXT_1) from T1 order by 1 desc
SELECT LOCATE(T1_TEXT_1, T1_TEXT_1) from T1 order by 1 desc
SELECT LOCATE("a", T1_TEXT_1) from T1 order by 1 desc
SELECT LOCATE("a", T1_INT_1) from T1 order by 1 desc
# some nullif,colla etc
SELECT NULLIF(T1_TEXT_1, T1_TEXT_2) from T1 order by 1 desc
SELECT COALESCE(T1_TEXT_1, T1_TEXT_2) from T1 order by 1 desc
#some with grammar
WITH tmp AS (SELECT T1_TEXT_1 a, T1_TEXT_2 b from T1 ) SELECT * from tmp where LENGTH(a) = LENGTH(b) order by 1 desc
WITH tmp AS (SELECT T1_TEXT_1 a from T1 UNION SELECT T2_TEXT_1 a from T2) SELECT * from tmp where LENGTH(a) = 5 order by 1 desc
#GROUP CONCAT
#SELECT MD5(GROUP_CONCAT(T1_TEXT_1)) from T1 order by 1 desc
SELECT substr(T1_CHAR_1,T1_CHAR_2, CONVERT(CASE T1_CHAR_1 WHEN cume_dist() OVER (ORDER BY T1_INT_1 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN T1_CHAR_1 END USING utf8mb4)) from T1 order by 1 desc
