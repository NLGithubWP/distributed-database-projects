

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



ALTER TABLE item SPLIT AT VALUES (40051), (80051);


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

