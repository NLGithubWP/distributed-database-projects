

-- server location: /home/stuproj/cs4224p/temp/tasks
-- Create db
DROP DATABASE IF EXISTS cs5424db CASCADE;
CREATE DATABASE IF NOT EXISTS cs5424db;
USE cs5424db;


-- set hash index enable to true
SET experimental_enable_hash_sharded_indexes=on;

-- load-splitting threshold to be 400
SET CLUSTER SETTING kv.range_split.by_load_enabled = true;
SET CLUSTER SETTING kv.range_split.load_qps_threshold = 1000;

-- Create user
-- local: CREATE USER IF NOT EXISTS rootuser WITH LOGIN PASSWORD 'rootuser';
-- server with following
CREATE USER IF NOT EXISTS rootuser;
