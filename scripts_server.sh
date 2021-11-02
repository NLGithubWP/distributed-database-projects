

server_num=5
server_port=27257

i=0
while [ $i -ne 40 ]
do
        server_id=`expr $i % $server_num`
        server_index=`expr 55 + $server_id`
        echo "$i" "xcnd$server_index:$server_port"
        ssh xcnd$server_index "python3 cockroachDB_driver.py -u postgresql://naili:@xcnd$server_index:$server_port/cs5424db -p /home/stuproj/cs4224p/temp/tasks/project_files_4/xact_files_A/$i.txt -w A >logs/python$i.log &"

        i=$(($i+1))
done
