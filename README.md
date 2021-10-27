# 
1. run python file http sever.

```bash
  python -m http.server 3000

```

2. Start a new clusters

```
rm -rf my-safe-directory
rm -rf certs
rm -rf node*

mkdir certs my-safe-directory

cockroach cert create-ca \
    --certs-dir=certs \
    --ca-key=my-safe-directory/ca.key

cockroach cert create-node \
    localhost \
    $(hostname) \
    --certs-dir=certs \
    --ca-key=my-safe-directory/ca.key

cockroach cert create-client \
    root \
    --certs-dir=certs \
    --ca-key=my-safe-directory/ca.key


cockroach start \
        --certs-dir=certs \
        --store=node1 \
        --listen-addr=localhost:26257 \
        --http-addr=localhost:8080 \
        --join=localhost:26257,localhost:26258,localhost:26259 \
        --background

cockroach start \
        --certs-dir=certs \
        --store=node2 \
        --listen-addr=localhost:26258 \
        --http-addr=localhost:8081 \
        --join=localhost:26257,localhost:26258,localhost:26259 \
        --background

cockroach start \
        --certs-dir=certs \
        --store=node3 \
        --listen-addr=localhost:26259 \
        --http-addr=localhost:8082 \
        --join=localhost:26257,localhost:26258,localhost:26259 \
        --background

cockroach init --certs-dir=certs --host=localhost:26257

ps aux | grep cock

```

3. preprocess csv file


```bash

python3 preprocess.py -f=<path to the folder where project_files is in>

```


4. run dbinit-workload-A.sql to load tables to test workloadA


```bash

cockroach sql \
    --host=localhost:26257 \
    --certs-dir=certs \
    --user=root \
    -f /Users/nailixing/Documents/NUS_Modules/CS5424_Distributed_Database/projects/CS5424/sqls/dbinit-workload-A.sql

```


5. run dbinit-workload-B.sql to load tables to test workloadB


```bash

cockroach sql \
    --host=localhost:26258 \
    --certs-dir=certs \
    --user=root \
    -f ./sqls/dbinit-workload-B.sql

```


5. run some sql

```bash

cockroach sql --certs-dir=certs --host=localhost:26257
cockroach sql --certs-dir=certs --host=localhost:26258
cockroach sql --certs-dir=certs --host=localhost:26259

```


