

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



INSERT INTO order_ori (O_W_ID, O_D_ID, O_ID, O_C_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) VALUES (4, 10, 3001, 255, 19, 0, now()) RETURNING O_ENTRY_D;


INSERT INTO order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) values (4,10,3002,0,93916,498,4,6,'S_DIST_10'),(4,10,3002,1,97959,308,4,7,'S_DIST_10'),(4,10,3002,2,79010,288,4,9,'S_DIST_10'),(4,10,3002,3,73443,96,4,4,'S_DIST_10'),(4,10,3002,4,5751,405,4,5,'S_DIST_10'),(4,10,3002,5,2085,144,4,8,'S_DIST_10'),(4,10,3002,6,5767,441,4,7,'S_DIST_10'),(4,10,3002,7,13091,150,4,10,'S_DIST_10'),(4,10,3002,8,14039,582,4,6,'S_DIST_10'),(4,10,3002,9,15582,94,4,2,'S_DIST_10'),(4,10,3002,10,21589,470,4,10,'S_DIST_10'),(4,10,3002,11,23764,216,4,6,'S_DIST_10'),(4,10,3002,12,50663,74,4,1,'S_DIST_10'),(4,10,3002,13,56743,18,4,3,'S_DIST_10'),(4,10,3002,14,64022,240,4,10,'S_DIST_10'),(4,10,3002,15,88415,99,4,9,'S_DIST_10'),(4,10,3002,16,93927,540,4,10,'S_DIST_10')


select * from order_line where (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID) in ((4,10,3002,0,93916));
