# Grammar file for testing IEJoin, the sort based algorithm for joins whose ON
# section has two inequality comparisons (ClickHouse PR #109920).
#
#   pstress-ch --grammar-sql=100 --grammar-file=clickhouse_iejoin_grammar.sql \
#              --compare-result-with-setting \
#              --run-query-setting "join_algorithm='ie_join,hash'"
#
# ie_join has to come FIRST in the list. Listed first it is used whenever the ON
# section has two inequality comparisons; listed last only when there is no
# equality condition, which would leave the equality + inequality shapes below
# running as a plain hash join in both passes and comparing nothing. Verified
# with EXPLAIN PLAN on 26.8.1:
#
#   default        -> cross join + filter, or SpillingHashJoin + post filter
#   'ie_join,hash' -> IEJoin for both shapes
#   'hash,ie_join' -> IEJoin only when there is no equality
#
# ---------------------------------------------------------------------------
# Two things make this file find bugs that a naive version does not.
#
# 1. ROW LEVEL, NOT COUNT LEVEL. Every line is wrapped in
#
#      SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM ( <the join> )
#
#    so the comparison covers every value of every joined row, not just how
#    many rows came back. A join that pairs the wrong rows but returns the right
#    number of them is a real bug and a bare count(*) cannot see it. The hash is
#    summed, so it is order insensitive and no ORDER BY is needed; the count
#    still pins cardinality. This is the same idiom pstress uses to compare
#    replicas in ch_verify.cpp.
#
# 2. TIE DENSE KEYS. A sort based inequality join lives or dies on rows whose
#    keys are equal, because that is exactly where < and <= differ. pstress
#    generates ints over a huge range, so two random rows almost never collide
#    and the interesting boundary is never reached. The "% 8" and "% 3" below
#    squeeze the keys into a handful of distinct values so ties are everywhere.
#    IEJoin still applies to expressions in ON, confirmed with EXPLAIN PLAN, so
#    this costs no coverage.
#
# Determinism: the wrapper makes every line return exactly one row, so ordering
# never matters. SEMI and ANTI joins take their right side columns from an
# arbitrary matching row, so those lines project the left side only.
#
# Cost: a pure inequality INNER join is a real cross product in the baseline
# pass. Fine at --records=1K, expensive at 10K and up.
# ---------------------------------------------------------------------------

# ---- equality + two inequalities, tie dense, every supported kind ----
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 INNER JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 LEFT JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 RIGHT JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 FULL JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2 FROM T1 SEMI LEFT JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2 FROM T1 ANTI LEFT JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T2_INT_1, T2_INT_2 FROM T1 SEMI RIGHT JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T2_INT_1, T2_INT_2 FROM T1 ANTI RIGHT JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)

# ---- the operator pairs, at the boundary where < and <= disagree ----
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 INNER JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 3 <= T2_INT_1 % 3 AND T1_INT_2 % 3 >= T2_INT_2 % 3)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 LEFT JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 3 <= T2_INT_1 % 3 AND T1_INT_2 % 3 >= T2_INT_2 % 3)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 FULL JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 3 < T2_INT_1 % 3 AND T1_INT_2 % 3 < T2_INT_2 % 3)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 INNER JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 3 >= T2_INT_1 % 3 AND T1_INT_2 % 3 <= T2_INT_2 % 3)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 RIGHT JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 3 <= T2_INT_1 % 3 AND T1_INT_2 % 3 < T2_INT_2 % 3)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T2_INT_1 FROM T1 INNER JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 % 2 <= T2_INT_1 % 2 AND T1_INT_2 % 2 >= T2_INT_2 % 2)

# ---- inequality keys that can be NULL: pstress makes Nullable columns ----
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T2_INT_1 FROM T1 INNER JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 < T2_INT_1 AND T1_INT_2 > T2_INT_2)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T2_INT_1 FROM T1 LEFT JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 < T2_INT_1 AND T1_INT_2 > T2_INT_2)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T2_INT_1 FROM T1 FULL JOIN T2 ON T1_INT_3 = T2_INT_3 AND T1_INT_1 < T2_INT_1 AND T1_INT_2 > T2_INT_2)

# ---- non int key types ----
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_DATE_1, T2_DATE_1 FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 AND T1_DATE_1 < T2_DATE_1 AND T1_DATE_2 > T2_DATE_2)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_DATETIME_1, T2_DATETIME_1 FROM T1 LEFT JOIN T2 ON T1_INT_1 = T2_INT_1 AND T1_DATETIME_1 < T2_DATETIME_1 AND T1_DATETIME_2 > T2_DATETIME_2)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_VARCHAR_1, T2_VARCHAR_1 FROM T1 INNER JOIN T2 ON T1_INT_1 = T2_INT_1 AND T1_VARCHAR_1 < T2_VARCHAR_1 AND T1_VARCHAR_2 > T2_VARCHAR_2)

# ---- INNER with inequalities only: cross join + filter against IEJoin ----
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 INNER JOIN T2 ON T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 INNER JOIN T2 ON T1_INT_1 % 3 <= T2_INT_1 % 3 AND T1_INT_2 % 3 >= T2_INT_2 % 3)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T1_INT_2, T2_INT_1, T2_INT_2 FROM T1 INNER JOIN T2 ON T1_INT_1 % 3 < T2_INT_1 % 3 AND T1_INT_2 % 3 < T2_INT_2 % 3)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T2_INT_1 FROM T1 INNER JOIN T2 ON T1_INT_1 < T2_INT_1 AND T1_INT_2 > T2_INT_2)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_DATE_1, T2_DATE_1 FROM T1 INNER JOIN T2 ON T1_DATE_1 < T2_DATE_1 AND T1_DATE_2 > T2_DATE_2)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T2_INT_1 FROM T1 INNER JOIN T2 ON T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8 WHERE T1_INT_3 > RAND_INT)

# ---- other kinds with inequalities only ----
# The baseline cannot run these at all: it fails with "Cannot determine join
# keys in JOIN ON expression", which is the pre-PR behaviour and expected. The
# ie_join pass still executes, so crashes and exceptions in the new operator
# surface, but there is nothing to compare against. They land in the "failed
# without the setting" counter at the end of the run, and in the thread log.
# To get a real oracle for these kinds instead, put
#   session: join_algorithm = 'ie_join,hash'
# in clickhouse_table_settings.txt so both passes use IEJoin, and set
#   --run-query-setting "max_block_size=7"
# so the two passes differ only in block size.
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T2_INT_1 FROM T1 LEFT JOIN T2 ON T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T2_INT_1 FROM T1 RIGHT JOIN T2 ON T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1, T2_INT_1 FROM T1 FULL JOIN T2 ON T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1 FROM T1 SEMI LEFT JOIN T2 ON T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T1_INT_1 FROM T1 ANTI LEFT JOIN T2 ON T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T2_INT_1 FROM T1 SEMI RIGHT JOIN T2 ON T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT T2_INT_1 FROM T1 ANTI RIGHT JOIN T2 ON T1_INT_1 % 8 < T2_INT_1 % 8 AND T1_INT_2 % 8 > T2_INT_2 % 8)
