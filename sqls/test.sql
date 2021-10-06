

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

