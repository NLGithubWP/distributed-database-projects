

server_num=5

i=0
while [ $i -ne 40 ]
do
        server_id=`expr $i % $server_num`
        server_ip=`expr 26257 + $server_id`
        echo "$i" "$server_ip"
        echo "python3 ../cockroachDB_driver.py -u postgresql://rootuser:@localhost:$server_ip/cs5424db -p /opt/project_files_4/xact_files_B/$i.txt -w B >../logs/python$i.log &"
        python3 cockroachDB_driver.py -u postgresql://rootuser:@localhost:$server_ip/cs5424db -p /opt/project_files_4/xact_files_B/$i.txt -w B >logs/python$i.log &
        i=$(($i+1))
done
