# CS5424

project

To use dbinit-workload-A.sql

firstly run a cockroachDB cluster

And then, run python file http sever.

```bash
  python -m http.server 3000

```

and then, can run dbinit-workload-A.sql to load tables, 


```bash

cockroach sql \
    --host=localhost:26258 \
    --certs-dir=certs \
    --user=root \
    -f ./sqls/dbinit-workload-A.sql


```
