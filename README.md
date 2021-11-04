# 1. run python file http sever before loading data

go to system root path and then run the python server

```bash
  cd /
  python3 -m http.server 3000
```

# 2.Start a new clusters with 5 instances

Please refer to office documents with following link

https://www.cockroachlabs.com/docs/stable/start-a-local-cluster.html

# 3.  Preprocess csv file


```bash
python3 preprocess.py -f=<path to the folder where project_files is in>
```

If the script "preprocess.py" and folder "project_files_4" are at the same folder. then can run directly. 

For example

```bash
python3 preprocess.py
```

# 4.  Run dbinit.sql to create database and user

```sql
cockroach sql \
    --host=xcnd55:27257 \
    --insecure \
    --user=root \
    -f <path to "dbinit.sql">
```

For example:

```sql
cockroach sql \
    --host=xcnd55:27257 \
    --insecure \
    --user=root \
    -f /home/stuproj/cs4224p/temp/tasks/sqls/dbinit.sql    
```

# 4.1. Run "dbinit-workload-A.sql" to load tables to test workloadA


```bash
cockroach sql \
    --host=xcnd55:27257 \
    --insecure \
    --user=root \
    -f <path to "dbinit-workload-A.sql">
```

For example:

```bash
cockroach sql \
    --host=xcnd55:27257 \
    --insecure \
    --user=root \
    -f /home/stuproj/cs4224p/temp/tasks/sqls/dbinit-workload-A.sql
```

# 4.2. Run "dbinit-workload-B.sql" to load tables to test workloadB


```bash
cockroach sql \
    --host=xcnd55:27257 \
    --insecure \
    --user=root \
    -f <path to "dbinit-workload-B.sql">
```

For example:

```bash
cockroach sql \
    --host=xcnd55:27257 \
    --insecure \
    --user=root \
    -f /home/stuproj/cs4224p/temp/tasks/sqls/dbinit-workload-B.sql
```

# 5. Run a single client driver

```bash
python3 cockroachDB_driver.py -u=<database url> -p=<path of workload files> -w=<workload_type>
```

For example

run a driver to read 22.txt under workloadA

```bash
python3 cockroachDB_driver.py -u postgresql://rootuser:@xcnd57:27257/cs5424db -p /home/stuproj/cs4224p/temp/tasks/project_files_4/xact_files_A/22.txt -w A
```

run a driver to read 22.txt under workloadB

```bash
python3 cockroachDB_driver.py -u postgresql://rootuser:@xcnd57:27257/cs5424db -p /home/stuproj/cs4224p/temp/tasks/project_files_4/xact_files_B/22.txt -w B
```

# 6. Run 40 clients on server

## Add configurations to config.py

```python
# workload is A or B
used_workload_type = "A"
# used_workload_type = "B"

# set folder path of project_files_4
folder_path = "/home/stuproj/cs4224p/temp/tasks"

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#   Those are mainly depends on the cluster setting,        #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# user name of database, this is default, dont update, otherwise, cannot do query.
db_user = "rootuser"

ip_list = ['xcnd55.comp.nus.edu.sg', 'xcnd56.comp.nus.edu.sg',          'xcnd57.comp.nus.edu.sg','xcnd58.comp.nus.edu.sg','xcnd59.comp.nus.edu.sg']

# user on server
user = "cs4224p"

# password to login to the server
password = "%Dwa2SL7"

# port of each cockroachDB instance
server_port = 27257
```

## Create empty csv files for output


```bash
python3 output/init_empty_csv.py
```

## Run script to launch 40 drivers across many nodes

```bash
python3 run_server.py
```

# 7. Generate statistical results

```
python3 output/get_throughput_dbstate.py -u=<database url> -w=<workload_type>
```

For example:

```bash
python3 output/get_throughput_dbstate.py -u=postgresql://rootuser:@xcnd57:27257/cs5424db -w=A
```

# 8. Run some sql (optional)

```bash
./cockroach sql --insecure --host=xcnd55:26257
```
