

# paswrod: 1234

%Dwa2SL7

for
    ssh cs4224p@xcnd55.comp.nus.edu.sg
    for file in range files:
      derver.read(file) (10) >> .log



cockroach start \
--insecure \
--store=node1 \
--listen-addr=xcnd55:27257 \
--http-addr=xcnd55:8080 \
--join=xcnd55:27257,xcnd56:27257,xcnd57:27257,xcnd58:27257,xcnd59:27257 \
--background



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


