

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


update customer set (C_BALANCE, C_DELIVERY_CNT) = ((select sum(ol_amount) from order_line where (ol_w_id, ol_d_id, ol_o_id) in ((1, 1, 2133)) group by ol_o_id), C_DELIVERY_CNT+1) where (c_w_id, c_d_id, c_id) in ((1, 1, 1879));


ps -ef | grep /appserver/jboss8080 | grep -v grep | awk  '{print $2}' | xargs  kill -9 >/dev/null 2>&1

