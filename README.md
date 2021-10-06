# CS5424

1. run python file http sever.

```bash
  python -m http.server 3000

```

2. Start clusters

```

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

```

3. run dbinit-workload-A.sql to load tables to test workloadA


```bash

cockroach sql \
    --host=localhost:26258 \
    --certs-dir=certs \
    --user=root \
    -f ./sqls/dbinit-workload-A.sql

```


4. run dbinit-workload-A.sql to load tables to test workloadB


```bash

cockroach sql \
    --host=localhost:26258 \
    --certs-dir=certs \
    --user=root \
    -f ./sqls/dbinit-workload-A.sql

```


5. run some sql

```bash

cockroach sql --certs-dir=certs --host=localhost:26257

```


