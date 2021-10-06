





SET experimental_enable_hash_sharded_indexes=on;
SET CLUSTER SETTING kv.range_split.by_load_enabled = true;
SET CLUSTER SETTING kv.range_split.load_qps_threshold = 3;
select * from item limit 100;
SHOW INDEX FROM item;
SHOW COLUMNS FROM item;
show ranges from table item;



ALTER TABLE item SPLIT AT VALUES (40051), (80051);
