cockroach start \
        --insecure \
        --store=node1 \
        --listen-addr=localhost:26257 \
        --http-addr=localhost:8280 \
        --join=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 \
        --background

cockroach start \
        --insecure \
        --store=node2 \
        --listen-addr=localhost:26258 \
        --http-addr=localhost:8281 \
        --join=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 \
        --background

cockroach start \
        --insecure \
        --store=node3 \
        --listen-addr=localhost:26259 \
        --http-addr=localhost:8282 \
        --join=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 \
        --background

cockroach start \
        --insecure \
        --store=node4 \
        --listen-addr=localhost:26260 \
        --http-addr=localhost:8283 \
        --join=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 \
        --background
cockroach start \
        --insecure \
        --store=node5 \
        --listen-addr=localhost:26261 \
        --http-addr=localhost:8284 \
        --join=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 \
        --background

cockroach init --insecure --host=localhost:26257

cockroach sql --insecure --host=localhost:26257

USE cs5424db;
set search_path to workloadA;
alter user rootuser with password rootuser;



cockroach sql \
    --host=localhost:26257 \
    --insecure \
    --user=root \
    -f /Users/nailixing/Documents/NUS_Modules/CS5424_Distributed_Database/projects/CS5424/sqls/dbinit.sql


ps -ef | grep cock | grep -v grep | awk  '{print $2}' | xargs  kill -9
