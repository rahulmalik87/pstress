# ClickHouse grammar SQL used by pstress (--grammar-file). This is the default
# grammar file for the ClickHouse build; grammar.sql is MySQL syntax and every
# line of it would error here.
#
# Placeholders, substituted by grammar_sql() in run_method.cpp:
#   T1, T2, ...            a random table, aliased to the placeholder name
#   T1_INT_1, T1_INT_2     int columns of that table (BIGINT -> Int64)
#   T1_VARCHAR_1           varchar column (-> String)
#   T1_CHAR_1, T1_TEXT_1   char / text column (-> String)
#   T1_DATE_1              date column (-> Date)
#   T1_DATETIME_1          datetime column (-> DateTime)
#   T1_INT_1=RAND          becomes "T1.icol1 = <random value of that column>"
#   RAND_INT, RAND_INT_0-9 a random integer 0-100
# A line needs T2 placeholders to get a second table; T1 alone joined to itself
# is a different (and much weaker) test.
#
# WARNING: with --compare-result-with-setting every line here runs twice and
# the two result sets are compared row by row, in order. So every line must be
# deterministic on stable data:
#
#   - either return a single row (a bare aggregate with no GROUP BY),
#   - or carry an ORDER BY over EVERY selected column. Ordering by only some of
#     them leaves ties free to come back in either order, which reads as a
#     result mismatch.
#
# For the same reason do not add ANY/SEMI/ASOF joins or a LIMIT without a full
# ORDER BY: those are allowed to pick an arbitrary row among equals, so they
# differ between two runs on their own, with no bug involved.
#
# Two more things that look like bugs but are not:
#   - sum()/avg() over a FLOAT or DOUBLE column. Float addition is not
#     associative, so a setting that changes the order rows are summed in
#     changes the last digits. Aggregate over int columns only.
#   - a subquery aliased back to a placeholder name, as in
#     "FROM (SELECT ... FROM T2) T2". The table substitution rewrites every T2
#     followed by a space or ')', including that alias, and the result is
#     double aliased and does not parse.

# ---- single row aggregates over a join ----
SELECT count(*) FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1
SELECT count(*) FROM T1 LEFT JOIN T2 ON T1_INT_1 = T2_INT_1
SELECT count(*) FROM T1 RIGHT JOIN T2 ON T1_INT_1 = T2_INT_1
SELECT count(*) FROM T1 FULL JOIN T2 ON T1_INT_1 = T2_INT_1
SELECT sum(T1_INT_1), count(*) FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1
SELECT sum(T1_INT_1), sum(T2_INT_1), count(*) FROM T1 LEFT JOIN T2 ON T1_INT_1 = T2_INT_1
SELECT min(T1_INT_1), max(T1_INT_1), count(*) FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1
SELECT count(*) FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 WHERE T1_INT_2 > RAND_INT
SELECT count(*) FROM T1 INNER JOIN T2 ON T1_DATE_1 = T2_DATE_1
SELECT count(*) FROM T1 INNER JOIN T2 ON T1_DATETIME_1 = T2_DATETIME_1
SELECT count(*) FROM T1 INNER JOIN T2 ON T1_VARCHAR_1 = T2_VARCHAR_1
SELECT sum(length(T1_TEXT_1)), count(*) FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1
SELECT count(*) FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 AND T1_INT_2 = T2_INT_2

# ---- multi row joins, ordered over every selected column ----
SELECT T1_INT_1, T2_INT_1 FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 ORDER BY T1_INT_1, T2_INT_1
SELECT T1_INT_1, T1_INT_2, T2_INT_1 FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 ORDER BY T1_INT_1, T1_INT_2, T2_INT_1
SELECT T1_INT_1, T2_VARCHAR_1 FROM T1 LEFT JOIN T2 ON T1_INT_1 = T2_INT_1 ORDER BY T1_INT_1, T2_VARCHAR_1
SELECT T1_DATE_1, T2_INT_1 FROM T1 INNER JOIN T2 ON T1_DATE_1 = T2_DATE_1 ORDER BY T1_DATE_1, T2_INT_1
SELECT T1_INT_1, T2_INT_1 FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 WHERE T1_INT_2 > RAND_INT ORDER BY T1_INT_1, T2_INT_1

# ---- grouped joins ----
SELECT T1_INT_1, count(*) FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 GROUP BY T1_INT_1 ORDER BY T1_INT_1, count(*)
SELECT T1_INT_1, sum(T2_INT_1) FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 GROUP BY T1_INT_1 ORDER BY T1_INT_1, sum(T2_INT_1)
SELECT T1_VARCHAR_1, count(*) FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 GROUP BY T1_VARCHAR_1 ORDER BY T1_VARCHAR_1, count(*)

# ---- three way joins ----
SELECT count(*) FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 INNER JOIN T3 ON T1_INT_2 = T3_INT_1
SELECT sum(T1_INT_1), count(*) FROM T1 LEFT JOIN T2 ON T1_INT_1 = T2_INT_1 LEFT JOIN T3 ON T1_INT_2 = T3_INT_1

# ---- subqueries in place of a join ----
SELECT count(*) FROM T1 WHERE T1_INT_1 IN (SELECT T2_INT_1 FROM T2)
SELECT count(*) FROM T1 WHERE T1_INT_1 NOT IN (SELECT T2_INT_1 FROM T2 WHERE T2_INT_2 > RAND_INT)
SELECT count(*) FROM T1 WHERE T1_INT_1 GLOBAL IN (SELECT T2_INT_1 FROM T2)
SELECT T1_INT_1 FROM T1 WHERE T1_INT_1 IN (SELECT T2_INT_1 FROM T2) ORDER BY T1_INT_1

# ---- single table, exercises the non join settings ----
SELECT count(*) FROM T1
SELECT sum(T1_INT_1), min(T1_INT_1), max(T1_INT_1), count(*) FROM T1
SELECT avg(T1_INT_1) FROM T1
SELECT T1_INT_1 FROM T1 ORDER BY T1_INT_1
SELECT T1_INT_1, T1_INT_2 FROM T1 WHERE T1_INT_1 > RAND_INT ORDER BY T1_INT_1, T1_INT_2
SELECT T1_INT_1, count(*) FROM T1 GROUP BY T1_INT_1 ORDER BY T1_INT_1, count(*)
SELECT count(*) FROM T1 WHERE T1_INT_1=RAND
SELECT uniqExact(T1_INT_1) FROM T1
