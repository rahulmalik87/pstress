SELECT count(1) from T1
#SELECT COUNT(1) from test.tt_1 t1 , test.tt_2 t2 where t1.ipkey=t2.ipkey;
#select length(v0), count(1) from tt_1 a group by length(v0) order by 1;
#select count(1) from tt_1 a where exists(select count(1) from tt_1 b where a.i1=b.i2);
#select count(1) from tt_1 group by ipkey having count(1)> 1;
#select count(1) from tt_1 where v1 not in (select v2 from tt_1 where v2 is not null) and v2 is not null;
#select count(1) from tt_1 t1 where v11 in (select v12 from tt_1 ) and v12 in (select v11 from tt_1 where v11 is not null) and v12 is not null;
#select count(1) from tt_1 t1 where i1 in (select i2 from tt_1 ) or i2 in (select i3 from tt_1 where i3 is not null);
