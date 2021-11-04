

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

CREATE INDEX s_joint_id ON stock (s_w_id, s_i_id);
CREATE INDEX c_joint_id ON customer (c_w_id, c_d_id, c_id);
CREATE INDEX d_joint_id ON district (d_w_id, d_id);
CREATE INDEX order_ori_joint_id ON order_ori (o_w_id, o_d_id, o_id, o_carrier_id);
CREATE INDEX order_ori_c_id ON order_ori (o_c_id);
CREATE INDEX order_ori_entry_d ON order_ori (o_entry_d);
CREATE INDEX order_line_joint_id ON order_line (ol_w_id, ol_d_id, ol_o_id, ol_number);


ALTER INDEX order_ori@order_ori_joint_c_id SPLIT AT VALUES (1,1),(1,10),(2,1),(2,10),(3,1),(3,10),(4,1),(4,10),(5,1),(5,10),(6,1),(6,10),(7,1),(7,10),(8,1),(8,10),(9,1),(9,10),(10,1),(10,10);

SET experimental_enable_hash_sharded_indexes=on;
SET CLUSTER SETTING kv.range_split.by_load_enabled = true;
SET CLUSTER SETTING kv.range_split.load_qps_threshold = 3;
select * from item limit 100;
SHOW INDEX FROM order_ori;
SHOW COLUMNS FROM order_ori;
show ranges from table order_ori;
SHOW CREATE order_ori;

