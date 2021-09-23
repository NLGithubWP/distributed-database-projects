# CS5424

project

To use load.sql

firstly run a cockroachDB cluster

And then, run python file http sever.

```bash
  python -m http.server 3000

```
and then run 

```bash
  cockroach sql --certs-dir=certs --host=localhost:26258

```
