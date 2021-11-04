

USE cs5424db;
set search_path to workloadA;
set search_path to workloadB;



show ranges from table order_ori;
show ranges from table customer;
show ranges from table order_line;
show ranges from table district;
show ranges from table item;
show ranges from table warehouse;
show ranges from table stock;
show ranges from table item_pair;



ALTER INDEX order_ori@order_ori_joint_c_id SPLIT AT VALUES (1,1),(1,10),(2,1),(2,10),(3,1),(3,10),(4,1),(4,10),(5,1),(5,10),(6,1),(6,10),(7,1),(7,10),(8,1),(8,10),(9,1),(9,10),(10,1),(10,10);


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




-- test now

SELECT O_ID, O_W_ID, O_D_ID FROM order_ori WHERE O_W_ID = 5 AND O_D_ID = 3 AND O_C_ID = 2289;


  o_id | o_w_id | o_d_id
-------+--------+---------
  3011 |      5 |      3
  1815 |      5 |      3
(2 rows)

Time: 4ms total (execution 4ms / network 0ms)


WITH
    items AS
        (SELECT OL_I_ID FROM order_line WHERE OL_O_ID = 3011 AND OL_W_ID = 5 AND OL_D_ID = 3),
    customer_ol AS
        (SELECT O_C_ID, O_W_ID, O_D_ID, O_ID, order_line.OL_I_ID
            FROM order_ori JOIN order_line
            ON O_W_ID = order_line.OL_W_ID AND O_D_ID = order_line.OL_D_ID AND O_ID = order_line.OL_O_ID
            WHERE O_W_ID <> 5 )
    SELECT DISTINCT O_W_ID, O_D_ID, O_C_ID
        FROM items LEFT JOIN customer_ol
        ON items.OL_I_ID = customer_ol.OL_I_ID
        GROUP BY O_C_ID, O_W_ID, O_D_ID, O_ID
        HAVING COUNT(*) >= 2;


WITH items AS (SELECT OL_I_ID FROM order_line WHERE OL_O_ID = 1815 AND OL_W_ID = 5 AND OL_D_ID = 3), customer_ol AS (SELECT O_C_ID, O_W_ID, O_D_ID, O_ID, order_line.OL_I_ID FROM order_ori JOIN order_line ON O_W_ID = order_line.OL_W_ID AND O_D_ID = order_line.OL_D_ID AND O_ID = order_line.OL_O_ID WHERE O_W_ID <> 5 ) SELECT DISTINCT O_W_ID, O_D_ID, O_C_ID FROM items LEFT JOIN customer_ol ON items.OL_I_ID = customer_ol.OL_I_ID GROUP BY O_C_ID, O_W_ID, O_D_ID, O_ID HAVING COUNT(*) >= 2 ;





-- optimization
-- find all order of a customer

ALTER TABLE cs5424db.workloadA.order_line
    ADD COLUMN ol_c_id INT FAMILY freqRead;


WITH dw AS
    (select o_c_id FROM order_ori JOIN order_line
            ON O_W_ID = order_line.OL_W_ID AND O_D_ID = order_line.OL_D_ID AND O_ID = order_line.OL_O_ID)

UPDATE customer SET C_W_NAME = W_NAME, C_D_NAME = D_NAME
    FROM dw WHERE customer.C_W_ID = dw.D_W_ID AND customer.C_D_ID = dw.D_ID;




WITH
    items AS
        (SELECT OL_I_ID FROM order_line WHERE OL_O_ID = 3011 AND OL_W_ID = 5 AND OL_D_ID = 3),
    customer_ol AS
        (SELECT O_C_ID, O_W_ID, O_D_ID, O_ID, order_line.OL_I_ID
            FROM order_ori JOIN order_line
            ON O_W_ID = order_line.OL_W_ID AND O_D_ID = order_line.OL_D_ID AND O_ID = order_line.OL_O_ID
            WHERE O_W_ID <> 5 )
    SELECT DISTINCT O_W_ID, O_D_ID, O_C_ID
        FROM items LEFT JOIN customer_ol
        ON items.OL_I_ID = customer_ol.OL_I_ID
        GROUP BY O_C_ID, O_W_ID, O_D_ID, O_ID
        HAVING COUNT(*) >= 2;

633


WITH
    items AS
        (SELECT OL_I_ID FROM order_line WHERE OL_O_ID = 3011 AND OL_W_ID = 5 AND OL_D_ID = 3),
    customer_ol AS
        (SELECT OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID FROM order_line WHERE OL_W_ID <> 5)
    SELECT DISTINCT OL_W_ID, OL_D_ID, OL_O_ID
        FROM items LEFT JOIN customer_ol
        ON items.OL_I_ID = customer_ol.OL_I_ID
        GROUP BY OL_W_ID, OL_D_ID, OL_O_ID
        HAVING COUNT(*) >= 2;



-- asdf

DEBUG:__main__:[ SELECT O_ID, O_W_ID, O_D_ID FROM order_ori WHERE O_W_ID = 5 AND O_D_ID = 3 AND O_C_ID = 2289 ]:   1585 us

3011, 1815


WITH
    items AS
        (SELECT OL_I_ID FROM order_line WHERE OL_O_ID = 1815 AND OL_W_ID = 5 AND OL_D_ID = 3),
    customer_ol AS
        (SELECT OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID FROM order_line WHERE OL_W_ID <> 5)
    SELECT DISTINCT OL_W_ID, OL_D_ID, OL_O_ID
        FROM items LEFT JOIN customer_ol
        ON items.OL_I_ID = customer_ol.OL_I_ID
        GROUP BY OL_W_ID, OL_D_ID, OL_O_ID
        HAVING COUNT(*) >= 2;

items: [19540, 74588, 84982,7340,56144,4278,48684,21205, 95623, 22291, 49304, 39650, 15673, 5330]



  ol_w_id | ol_d_id | ol_o_id
----------+---------+----------
       10 |       7 |     373
(1 row)



select distinct O_W_ID, O_D_ID, o_c_id from order_ori where (O_W_ID, O_D_ID, O_ID) in ((10, 7, 373)))

  o_w_id | o_d_id | o_c_id
---------+--------+---------
      10 |      7 |    211


give ordered items :


combe_list = [(),(),(),()]

SELECT OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID FROM order_line WHERE OL_W_ID <> 5 and



with
    match_order_item AS
    (select OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID from order_line where ol_i_id in (19540, 74588, 84982,7340,56144,4278,48684,21205, 95623, 22291, 49304, 39650, 15673, 5330) and OL_W_ID <> 5)
    select OL_W_ID, OL_D_ID, OL_O_ID from match_order_item group by OL_W_ID, OL_D_ID, OL_O_ID having count(*) >= 2;





with
    match_order_item AS
    (select OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID from order_line where ol_i_id in (SELECT OL_I_ID FROM order_line WHERE OL_O_ID = 1815 AND OL_W_ID = 5 AND OL_D_ID = 3) and OL_W_ID <> 5)
    select OL_W_ID, OL_D_ID, OL_O_ID from match_order_item group by OL_W_ID, OL_D_ID, OL_O_ID having count(*) >= 2;




select ol_w_id, ol_d_id, ol_o_id, ol_i_id from order_line where (ol_w_id, ol_d_id, ol_o_id) in ((10, 7, 373));


       10 |       7 |     373 |   14856
       10 |       7 |     373 |   37413
       10 |       7 |     373 |    6353
       10 |       7 |     373 |   20365
       10 |       7 |     373 |   84596
       10 |       7 |     373 |   53749
       10 |       7 |     373 |   82241
       10 |       7 |     373 |   19540
       10 |       7 |     373 |    5668
       10 |       7 |     373 |   64667
       10 |       7 |     373 |    6529
       10 |       7 |     373 |   48405
       10 |       7 |     373 |   86295
       10 |       7 |     373 |   96822
       10 |       7 |     373 |   63302
       10 |       7 |     373 |   39650
       10 |       7 |     373 |   15024
       10 |       7 |     373 |   76179
       10 |       7 |     373 |   88886


with
    match_order_item AS
        (select OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID
            from order_line
            where ol_i_id in (SELECT OL_I_ID FROM order_line
                               WHERE OL_O_ID = 66 AND OL_W_ID = 9 AND OL_D_ID = 8)
             AND OL_W_ID <> 9)
    select OL_W_ID, OL_D_ID, OL_O_ID from match_order_item group by OL_W_ID, OL_D_ID, OL_O_ID having count(*) >= 2;



select ol_i_id from order_line where ol_w_id = 9 and ol_d_id = 10 and ol_o_id = 1490 order by ol_i_id;
  ol_i_id
-----------
     2118
     9135
    10074
    23360
        25808
        47340
    50167
    51764
        52354
        52957
    53284
        53642
    58409
        69990
    83763
    86009
    88077
        91206
    94727

input value, [w_id, d_id, c_id] are
9 10 2213
target customer's (w_id, d_id, c_id, o_id): matched customer's (OL_W_ID, OL_D_ID, OL_O_ID)
{(9, 10, 2213, 1490): [(4, 6, 369), (4, 5, 473), (5, 3, 2739), (2, 6, 758)]}
-- And then, check c_id from order_ori table according to matched customer's (OL_W_ID, OL_D_ID, OL_O_ID)
-- (4, 6, 369, 1492)
-- (5, 3, 2739, 1184)
-- (4, 5, 473, 2109)
-- (2, 6, 758, 371)
-- Related customers (W_ID, D_ID, C_ID):
-- 4 6 1492
-- 5 3 1184
-- 4 5 2109
-- 2 6 371

select o_c_id from order_ori where (o_w_id, o_d_id, o_id) in ((4, 6, 369));
select o_c_id from order_ori where (o_w_id, o_d_id, o_id) in ((4, 5, 473));
select o_c_id from order_ori where (o_w_id, o_d_id, o_id) in ((5, 3, 2739));
select o_c_id from order_ori where (o_w_id, o_d_id, o_id) in ((2, 6, 758));

check (4, 6, 369):

select ol_i_id from order_line where (ol_w_id, ol_d_id, ol_o_id) in ((4, 6, 369)) order by ol_i_id;
     6391
     9752
    11202
    13963
    32106
    40796
    44993
        47340
    48738
        52354
    53503
    53646
    54877
    57178
    70966
    74705
    97784
    99167

select ol_i_id from order_line where (ol_w_id, ol_d_id, ol_o_id) in ((4, 5, 473)) order by ol_i_id;
     1863
     5019
    12677
    22594
    22784
    24827
    37923
        52957
    59210
    70034
    72649
    75314
    79034
    83886
    89983
    90890
        91206
    97633
    98706


select ol_i_id from order_line where (ol_w_id, ol_d_id, ol_o_id) in ((5, 3, 2739)) order by ol_i_id;
    10657
    14795
    18179
    24915
        25808
    36981
    40270
    52441
        53642
    56733
    67607
    73247
    74049
    76522
    77020
    81012
    84565
    86766


select ol_i_id from order_line where (ol_w_id, ol_d_id, ol_o_id) in ((2, 6, 758)) order by ol_i_id;

     6605
     9647
    15172
    23800
    34036
    40572
    41727
    44554
        52354
    53932
    55372
        69990
    79085
    90912

SELECT D_NEXT_O_ID FROM district WHERE D_W_ID = 7 AND D_ID = 4

SELECT COUNT(DISTINCT OL_I_ID)
    FROM order_line JOIN stock ON OL_I_ID = stock.S_I_ID AND OL_W_ID = stock.S_W_ID
    WHERE OL_W_ID = 7 AND OL_D_ID = 4 AND OL_O_ID >= 2990 AND OL_O_ID < 3001 AND S_QUANTITY < '14'


DEBUG:__main__:[SET TRANSACTION AS OF SYSTEM TIME '-5s']:   550 us
DEBUG:__main__:[SELECT D_NEXT_O_ID FROM district WHERE D_W_ID = 5 AND D_ID = 3]:   1423 us
DEBUG:__main__:[ SELECT O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST FROM order_ori JOIN customer ON order_ori.O_W_ID=customer.C_W_ID AND order_ori.O_D_ID=customer.C_D_ID AND order_ori.O_C_ID=customer.C_ID WHERE O_W_ID = 5 AND O_D_ID = 3 AND O_ID >= 3012 AND O_ID < 3026 ORDER BY O_ID ]:   5434 us
DEBUG:__main__:[WITH ol2 AS (SELECT OL_O_ID, OL_W_ID, OL_D_ID, MAX(OL_QUANTITY) AS MAX FROM order_line WHERE OL_W_ID = 5 AND OL_D_ID = 3 AND OL_O_ID >= 3012 AND OL_O_ID < 3026 GROUP BY OL_O_ID, OL_W_ID, OL_D_ID) SELECT order_line.OL_O_ID, order_line.OL_I_ID, item.I_NAME, order_line.OL_QUANTITY FROM order_line JOIN item ON order_line.OL_I_ID=item.I_ID INNER JOIN ol2 ON order_line.OL_O_ID=ol2.OL_O_ID AND order_line.OL_W_ID=ol2.OL_W_ID AND order_line.OL_D_ID=ol2.OL_D_ID AND OL_QUANTITY=MAX ORDER BY order_line.OL_O_ID ]:   3823 us


ERROR:__main__:Errored: Unknown Error in running tx: PopItemTxParams, ErrorMsg:
[ list index out of range ], Traceback:
[ Traceback (most recent call last):
  File "cockroachDB_driver.py", line 64, in run_tx
    res = operation(m_conn)
  File "cockroachDB_driver.py", line 219, in <lambda>
    lambda l_conn: m_tx_ins.popular_item_transaction(l_conn, m_params))
  File "/Users/rootuserxing/Documents/NUS_Modules/CS5424_Distributed_Database/projects/CS5424/txs/tx_workloadA.py", line 248, in popular_item_transaction
    while p_x[j][0] == s[i][0]:
IndexError: li


n-l = 3012
n = 3026
l = 14


-- last order
SELECT D_NEXT_O_ID FROM district WHERE D_W_ID = 5 AND D_ID = 3

-- order and customer
SELECT O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST
    FROM order_ori JOIN customer
    ON order_ori.O_W_ID=customer.C_W_ID AND order_ori.O_D_ID=customer.C_D_ID AND order_ori.O_C_ID=customer.C_ID
    WHERE O_W_ID = 5 AND O_D_ID = 3 AND O_ID >= 3012 AND O_ID < 3026 ORDER BY O_ID;

select * from order_ori WHERE O_W_ID = 5 AND O_D_ID = 3 AND O_ID >= 3012 AND O_ID < 3026;
select * from order_line WHERE OL_W_ID = 5 AND OL_D_ID = 3 AND OL_O_ID >= 3012 AND OL_O_ID < 3026;

-- order line
WITH ol2 AS
    (SELECT OL_O_ID, OL_W_ID, OL_D_ID, MAX(OL_QUANTITY) AS MAX
        FROM order_line
        WHERE OL_W_ID = 5 AND OL_D_ID = 3 AND OL_O_ID >= 3012 AND OL_O_ID < 3026
        GROUP BY OL_O_ID, OL_W_ID, OL_D_ID)

    SELECT order_line.OL_O_ID, order_line.OL_I_ID, item.I_NAME, order_line.OL_QUANTITY
        FROM order_line
        JOIN item
        ON order_line.OL_I_ID=item.I_ID
        INNER JOIN ol2
        ON order_line.OL_O_ID=ol2.OL_O_ID AND order_line.OL_W_ID=ol2.OL_W_ID AND order_line.OL_D_ID=ol2.OL_D_ID AND OL_QUANTITY=MAX
        ORDER BY order_line.OL_O_ID;

(5,3,3018,315,17,1)

       pid                              | o_w_id | o_d_id | o_id | o_c_id | o_carrier_id | o_ol_cnt | o_all_local |           o_entry_d
---------------------------------------+--------+--------+------+--------+--------------+----------+-------------+--------------------------------
  063d7c5c-49fa-4163-a013-688dc2c5f4d1 |      5 |      3 | 3018 |    315 |         NULL |       17 |           1 | 2021-11-02 04:21:01.118621+00


UPDATE district SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = 1 and D_ID = 2 returning D_NEXT_O_ID, d_tax;


USE cs5424db;
set search_path to workloadA;

UPDATE district SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = 1 and D_ID = 3 returning D_NEXT_O_ID, d_tax;

select * from order_ori where O_W_ID=1 and O_D_ID=2 and O_ID=3841;
select * from order_line where OL_W_ID = 1 AND OL_D_ID = 2 AND OL_O_ID =3841;



select * from order_ori where O_W_ID=1 and O_D_ID=2 and O_ID=3844;

select * from district where d_w_id = 1 and d_id = 1;


select count(distinct o_id) from order_ori;


select * from order_ori LEFT join order_line on o_w_id = ol_w_id and o_d_id = ol_d_id and o_id = ol_o_id where ol_i_id = null;
