

#python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:26257/cs5424db?sslmode=require -p /opt/project_files_4/xact_files_A/0.txt -w A >logs/python1.log &
#python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:26257/cs5424db?sslmode=require -p /opt/project_files_4/xact_files_A/1.txt -w A >logs/python1.log &
#python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:26257/cs5424db?sslmode=require -p /opt/project_files_4/xact_files_A/2.txt -w A >logs/python2.log &
#python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:26258/cs5424db?sslmode=require -p /opt/project_files_4/xact_files_A/3.txt -w A >logs/python3.log &
#python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:26258/cs5424db?sslmode=require -p /opt/project_files_4/xact_files_A/4.txt -w A >logs/python4.log &
#python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:26259/cs5424db?sslmode=require -p /opt/project_files_4/xact_files_A/5.txt -w A >logs/python5.log &
#python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:26259/cs5424db?sslmode=require -p /opt/project_files_4/xact_files_A/6.txt -w A >logs/python6.log &
#python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:26257/cs5424db?sslmode=require -p /opt/project_files_4/xact_files_A/7.txt -w A >logs/python7.log &
#python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:26258/cs5424db?sslmode=require -p /opt/project_files_4/xact_files_A/8.txt -w A >logs/python8.log &


server_num=5

i=0
while [ $i -ne 40 ]
do
        server_ip=27257
        echo "$i" "$server_ip"
        echo "python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:$server_ip/cs5424db?sslmode=require -p /opt/project_files_4/xact_files_A/$i.txt -w A >logs/python$i.log &"
        python3 cockroachDB_driver.py -u postgresql://naili:naili@localhost:$server_ip/cs5424db?sslmode=require -p /home/stuproj/cs4224p/temp/tasks/project_files_4/xact_files_A/$i.txt -w A >logs/python$i.log &
        i=$(($i+1))
done
/home/stuproj/cs4224p/temp/tasks

