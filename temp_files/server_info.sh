

# paswrod: 1234

%Dwa2SL7

ssh cs4224p@xcnd55.comp.nus.edu.sg




./cockroach quit --insecure --host=xcnd55:27257
./cockroach quit --insecure --host=xcnd56:27257
./cockroach quit --insecure --host=xcnd57:27257
./cockroach quit --insecure --host=xcnd58:27257
./cockroach quit --insecure --host=xcnd59:27257

cockroach start \
  --insecure \
  --store=node1 \
  --listen-addr=xcnd55:27257 \
  --http-addr=xcnd55:8080 \
  --join=xcnd55:27257,xcnd56:27257,xcnd57:27257,xcnd58:27257,xcnd59:27257 \
  --background

cockroach start \
  --insecure \
  --store=node2 \
  --listen-addr=xcnd56:27257 \
  --http-addr=xcnd56:8080 \
  --join=xcnd55:27257,xcnd56:27257,xcnd57:27257,xcnd58:27257,xcnd59:27257 \
  --background

cockroach start \
--insecure \
--store=node3 \
--listen-addr=xcnd57:27257 \
--http-addr=xcnd57:8080 \
--join=xcnd55:27257,xcnd56:27257,xcnd57:27257,xcnd58:27257,xcnd59:27257 \
--background

cockroach start \
--insecure \
--store=node4 \
--listen-addr=xcnd58:27257 \
--http-addr=xcnd58:8080 \
--join=xcnd55:27257,xcnd56:27257,xcnd57:27257,xcnd58:27257,xcnd59:27257 \
--background

cockroach start \
  --insecure \
  --store=node5 \
  --listen-addr=xcnd59:27257 \
  --http-addr=xcnd59:8080 \
  --join=xcnd55:27257,xcnd56:27257,xcnd57:27257,xcnd58:27257,xcnd59:27257 \
  --background


cockroach init --insecure --host=xcnd55:27257


cockroach sql \
    --host=xcnd55:27257 \
    --insecure \
    --user=root \
    -f /home/stuproj/cs4224p/temp/tasks/sqls/dbinit-workload-A.sql


cockroach sql --insecure --host=xcnd55:27257

USE cs5424db;
set search_path to workloadA;
show tables;


cockroach sql \
    --host=xcnd55:27257 \
    --insecure \
    --user=root \
    -f /home/stuproj/cs4224p/temp/tasks/sqls/dbinit-workload-A.sql


python3 cockroachDB_driver.py -u postgresql://rootuser:@xcnd55:27257/cs5424db -p /home/stuproj/cs4224p/temp/tasks/project_files_4/xact_files_A/1.txt -w A >logs/python100.log &

export PYTHONPATH=$(python3 -c "import site, os; print(os.path.join(site.USER_BASE, 'lib', 'python3.6', 'site-packages'))"):$PYTHONPATH

addr = "postgresql://rootuser:@xcnd55:27257/cs5424db?sslmode=require"
