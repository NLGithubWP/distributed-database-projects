

server_num=5

i=20
while [ $i -ne 30 ]
do
        server_id=`expr $i % $server_num`
        server_ip=`expr 26257 + $server_id`
        echo "$i" "$server_ip"
        echo "python3 cockroachDB_driver.py -u postgresql://naili:@localhost:$server_ip/cs5424db -p /opt/project_files_4/xact_files_B/$i.txt -w A >logs/python$i.log &"
        python3 cockroachDB_driver.py -u postgresql://naili:@localhost:$server_ip/cs5424db -p /opt/project_files_4/xact_files_B/$i.txt -w A >logs/python$i.log &
        i=$(($i+1))
done
