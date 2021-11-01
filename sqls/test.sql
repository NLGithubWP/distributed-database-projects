

USE cs5424db;
set search_path to workloadA;



SET experimental_enable_hash_sharded_indexes=on;
SET CLUSTER SETTING kv.range_split.by_load_enabled = true;
SET CLUSTER SETTING kv.range_split.load_qps_threshold = 3;
select * from item limit 100;
SHOW INDEX FROM order_ori;
SHOW COLUMNS FROM order_ori;
show ranges from table order_ori;
SHOW CREATE order_ori;

ALTER INDEX rides@revenue_idx SPLIT AT VALUES (25.00), (50.00), (75.00);


ALTER TABLE item SPLIT AT VALUES (40051), (80051);

ALTER TABLE customer SPLIT AT VALUES (1,1),(1,10),(2,1),(2,10),(3,1),(3,10),(4,1),(4,10),(5,1),(5,10),(6,1),(6,10),(7,1),(7,10),(8,1),(8,10),(9,1),(9,10),(10,1),(10,10);

ALTER TABLE customer SPLIT AT VALUES (1,1),(1,10),(2,1),(2,10),(3,1),(3,10),(4,1),(4,10),(5,1),(5,10),(6,1),(6,10),(7,1),(7,10),(8,1),(8,10),(9,1),(9,10),(10,1),(10,10);

ALTER INDEX order_ori@order_ori_joint_c_id SPLIT AT VALUES (1,1),(1,10),(2,1),(2,10),(3,1),(3,10),(4,1),(4,10),(5,1),(5,10),(6,1),(6,10),(7,1),(7,10),(8,1),(8,10),(9,1),(9,10),(10,1),(10,10);


pid UUID DEFAULT gen_random_uuid() PRIMARY KEY,


CREATE INDEX ON cs5424db.workloadA.order_ori (o_id);


update order_ori set o_carrier_id = 12 where (o_w_id, o_d_id, o_id) in
(select o_w_id, o_d_id, o_id from order_ori where o_w_id = 1 and o_d_id = 1 and o_id =
    (select MIN(o_id) from order_ori
        where o_w_id = 1 and o_d_id = 1 and o_carrier_id is null) and o_carrier_id is null)
        returning o_id, o_c_id;



select o_w_id, o_d_id, o_id from order_ori
    where o_w_id = 7 and o_d_id = 14 and o_carrier_id is null
    order by o_id limit 1;

WITH
    Xorder AS
    (select o_id from order_ori
        where o_w_id = 1 and o_d_id = 1 and o_carrier_id is null order by o_id limit 1)
    update order_ori set o_carrier_id = 12 where o_w_id = 1 and o_d_id = 1 and o_id = Xorder.min and o_carrier_id is null;



update order_ori set o_carrier_id = 8 where (o_w_id, o_d_id, o_id) in
        (select o_w_id, o_d_id, o_id from order_ori where o_w_id = 1 and o_d_id = 1 and o_id =
                (select MIN(o_id) from order_ori where o_w_id = 1 and o_d_id = 1 and o_carrier_id is null) and o_carrier_id is null for update) returning o_id, o_c_id;


select o_id from order_ori where o_w_id = 1 and o_d_id = 1 and o_carrier_id is null order by o_id limit 1 for update;

update order_ori set o_carrier_id = 8 where o_w_id = 1 and o_d_id = 1 and o_id = (select MIN(o_id) from order_ori where o_w_id = 1 and o_d_id = 1 and o_carrier_id is null) returning o_id, o_c_id;


update tb set col=col+1
where col=(select max(col) from tb)



update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line where (ol_w_id, ol_d_id, ol_o_id) in ((1, 1, 2133)) group by ol_o_id), C_DELIVERY_CNT+1) where (c_w_id, c_d_id, c_id) in ((1, 1, 1879));


ps -ef | grep /appserver/jboss8080 | grep -v grep | awk  '{print $2}' | xargs  kill -9 >/dev/null 2>&1


update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line where (ol_w_id, ol_d_id, ol_o_id) in ((1, 1, 2179)) group by ol_o_id), C_DELIVERY_CNT+1) where ( ) in ((1, 1, 604));



select o_w_id, o_d_id, o_c_id, count(*) from order_ori group by (o_w_id, o_d_id, o_c_id) having count(*) > 1

SELECT O_ID, O_W_ID, O_D_ID FROM order_ori AS OF SYSTEM TIME '-10m' WHERE O_W_ID = 1 AND O_D_ID = 4 AND O_C_ID = 2239 ;
AS OF SYSTEM TIME '-4h'

SELECT O_ID, O_W_ID, O_D_ID FROM order_ori AS OF SYSTEM TIME '-4m' WHERE O_W_ID = 1 AND O_D_ID = 4 AND O_C_ID = 2239 ;









update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=1 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=1 and o_carrier_id is null)  returning o_id, o_c_id;   11032 us
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=1 and ol_o_id=2124;   6246 us
update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line    where (ol_w_id, ol_d_id, ol_o_id) in ((8, 1, 2124)) group by ol_o_id), C_DELIVERY_CNT+1)    where (c_w_id, c_d_id, c_id) in ((8, 1, 2908));   5265 us

update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=2 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=2 and o_carrier_id is null)  returning o_id, o_c_id;   12994 us
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=2 and ol_o_id=2124;   4264 us
update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line    where (ol_w_id, ol_d_id, ol_o_id) in ((8, 2, 2124)) group by ol_o_id), C_DELIVERY_CNT+1)    where (c_w_id, c_d_id, c_id) in ((8, 2, 2632));   5024 us

update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=3 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=3 and o_carrier_id is null)  returning o_id, o_c_id;   10446 us
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=3 and ol_o_id=2124;   8045 us
update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line    where (ol_w_id, ol_d_id, ol_o_id) in ((8, 3, 2124)) group by ol_o_id), C_DELIVERY_CNT+1)    where (c_w_id, c_d_id, c_id) in ((8, 3, 2526));   5019 us

update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=4 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=4 and o_carrier_id is null)  returning o_id, o_c_id;   9946 us
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=4 and ol_o_id=2124;   6148 us
update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line    where (ol_w_id, ol_d_id, ol_o_id) in ((8, 4, 2124)) group by ol_o_id), C_DELIVERY_CNT+1)    where (c_w_id, c_d_id, c_id) in ((8, 4, 365));   5954 us

update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=5 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=5 and o_carrier_id is null)  returning o_id, o_c_id;   9311 us
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=5 and ol_o_id=2124;   10055 us
update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line    where (ol_w_id, ol_d_id, ol_o_id) in ((8, 5, 2124)) group by ol_o_id), C_DELIVERY_CNT+1)    where (c_w_id, c_d_id, c_id) in ((8, 5, 1517));   5592 us

update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=6 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=6 and o_carrier_id is null)  returning o_id, o_c_id;   9071 us
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=6 and ol_o_id=2124;   9883 us
update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line    where (ol_w_id, ol_d_id, ol_o_id) in ((8, 6, 2124)) group by ol_o_id), C_DELIVERY_CNT+1)    where (c_w_id, c_d_id, c_id) in ((8, 6, 1074));   5198 us

update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=7 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=7 and o_carrier_id is null)  returning o_id, o_c_id;   6796 us
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=7 and ol_o_id=2124;   7699 us
update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line    where (ol_w_id, ol_d_id, ol_o_id) in ((8, 7, 2124)) group by ol_o_id), C_DELIVERY_CNT+1)    where (c_w_id, c_d_id, c_id) in ((8, 7, 2109));   4680 us

update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=8 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=8 and o_carrier_id is null)  returning o_id, o_c_id;   7550 us
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=8 and ol_o_id=2124;   4546 us
update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line    where (ol_w_id, ol_d_id, ol_o_id) in ((8, 8, 2124)) group by ol_o_id), C_DELIVERY_CNT+1)    where (c_w_id, c_d_id, c_id) in ((8, 8, 252));   3487 us

update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=9 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=9 and o_carrier_id is null)  returning o_id, o_c_id;   9935 us
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=9 and ol_o_id=2124;   7227 us
update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line    where (ol_w_id, ol_d_id, ol_o_id) in ((8, 9, 2124)) group by ol_o_id), C_DELIVERY_CNT+1)    where (c_w_id, c_d_id, c_id) in ((8, 9, 1861));   5695 us

update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=10 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=10 and o_carrier_id is null)  returning o_id, o_c_id;   9348 us
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=10 and ol_o_id=2124;   6026 us
update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line    where (ol_w_id, ol_d_id, ol_o_id) in ((8, 10, 2124)) group by ol_o_id), C_DELIVERY_CNT+1)    where (c_w_id, c_d_id, c_id) in ((8, 10, 1334));   6155 us



update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=1 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=1 and o_carrier_id is null)  returning o_id, o_c_id;
update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=2 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=2 and o_carrier_id is null)  returning o_id, o_c_id;
update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=3 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=3 and o_carrier_id is null)  returning o_id, o_c_id;
update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=4 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=4 and o_carrier_id is null)  returning o_id, o_c_id;
update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=5 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=5 and o_carrier_id is null)  returning o_id, o_c_id;
update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=6 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=6 and o_carrier_id is null)  returning o_id, o_c_id;
update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=7 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=7 and o_carrier_id is null)  returning o_id, o_c_id;
update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=8 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=8 and o_carrier_id is null)  returning o_id, o_c_id;
update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=9 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=9 and o_carrier_id is null)  returning o_id, o_c_id;
update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=10 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=10 and o_carrier_id is null)  returning o_id, o_c_id;
update order_ori set o_carrier_id=10 where o_w_id=8 and o_d_id=1 and o_id=(select MIN(o_id) from order_ori where o_w_id=8 and o_d_id=1 and o_carrier_id is null)  returning o_id, o_c_id;



select o_d_id, MIN(o_id) from order_ori group by (o_w_id, o_d_id, o_carrier_id) having o_w_id = 8 and o_carrier_id is null;

update order_ori set o_carrier_id=10 where (o_w_id, o_d_id, o_id) in ((8,1,2127), (8,2,2127), (8,3,2127),(8,4,2127),(8,5,2127),(8,6,2127),(8,7,2127),(8,8,2127),(8,9,2127),(8,10,2127)) returning o_d_id, o_id, o_c_id;

 o_d_id | o_id | o_c_id
---------+------+---------
       2 | 2127 |   1908
       9 | 2127 |   2819
       4 | 2127 |    258
       7 | 2127 |    647
       6 | 2127 |   2504
       1 | 2127 |   1413
       8 | 2127 |   2260
       3 | 2127 |   1746
      10 | 2127 |   2413
       5 | 2127 |   2567

update order_line set OL_DELIVERY_D =now() where (ol_w_id, ol_d_id, ol_o_id) in ((8,1,2127), (8,2,2127), (8,3,2127),(8,4,2127),(8,5,2127),(8,6,2127),(8,7,2127),(8,8,2127),(8,9,2127),(8,10,2127));

select ol_w_id, ol_d_id, sum(ol_amount) from order_line where (ol_w_id, ol_d_id, ol_o_id) in ((8,1,2127), (8,2,2127), (8,3,2127),(8,4,2127),(8,5,2127),(8,6,2127),(8,7,2127),(8,8,2127),(8,9,2127),(8,10,2127)) group by (ol_w_id, ol_d_id, ol_o_id);

  ol_w_id | ol_d_id | ol_o_id |    sum
----------+---------+---------+------------
        8 |       1 |    2127 |  56418.59
        8 |       2 |    2127 |  24863.09
        8 |       3 |    2127 | 108863.71
        8 |       4 |    2127 |  60009.36
        8 |       5 |    2127 |  68234.26
        8 |       6 |    2127 |  55682.95
        8 |       7 |    2127 |  79482.72
        8 |       8 |    2127 |  41021.69
        8 |       9 |    2127 |  31815.16
        8 |      10 |    2127 |  72008.03


update customer set (C_BALANCE, C_DELIVERY_CNT) = (56418.59,C_DELIVERY_CNT+1) where (c_w_id, c_d_id, c_id) in
((8, 1, 56418.59),(8, 2,  24863.09),(8, 3, 108863.71),(8, 4, 60009.36),(8, 5, 68234.26),(8, 6, 55682.95),(8, 7, 79482.72),(8, 8, 41021.69),(8, 9, 31815.16),(8, 10, 72008.03);

update customer set (C_BALANCE, C_DELIVERY_CNT) = (56418.59,C_DELIVERY_CNT+1),  where (c_w_id, c_d_id, c_id) in ((8, 1, 56418.59),(8, 2,  24863.09);

update customer
    set (C_BALANCE, C_DELIVERY_CNT) = CASE when (c_w_id, c_d_id, c_id) = (8, 1, 1413) then (56418.59,C_DELIVERY_CNT+1) when (c_w_id, c_d_id, c_id) = (8, 2, 1908) then (24863.09,C_DELIVERY_CNT+1)




with selected_order_ori AS
    (select o_w_id, o_d_id, o_carrier_id, MIN(o_id) from order_ori group by (o_w_id, o_d_id, o_carrier_id) having o_w_id = 8 and o_carrier_id is null)
update selected_order_ori set o_carrier_id=10 where o_w_id=8;


update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=1 and ol_o_id=2124
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=2 and ol_o_id=2124
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=3 and ol_o_id=2124
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=4 and ol_o_id=2124
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=5 and ol_o_id=2124
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=6 and ol_o_id=2124
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=7 and ol_o_id=2124
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=8 and ol_o_id=2124
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=9 and ol_o_id=2124
update order_line set OL_DELIVERY_D =now() where ol_w_id=8 and ol_d_id=10 and ol_o_id=2124



update customer set (C_BALANCE, C_DELIVERY_CNT) = (
       (select sum(ol_amount) from order_line where (ol_w_id, ol_d_id, ol_o_id) in ((8, 10, 2124)) group by ol_o_id),
        C_DELIVERY_CNT+1)
    where (c_w_id, c_d_id, c_id) in ((8, 10, 1334));   6155 us





