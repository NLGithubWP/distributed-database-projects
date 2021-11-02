
import paramiko


# definitions for ssh connection and cluster
ip_list = ['xcnd55.comp.nus.edu.sg',
           'xcnd56.comp.nus.edu.sg',
           'xcnd57.comp.nus.edu.sg',
           'xcnd58.comp.nus.edu.sg',
           'xcnd59.comp.nus.edu.sg']


user = "cs4224p"
password = "%Dwa2SL7"
server_base_index = 55
server_port = 27257

stdouts = []
clients = []

# Start the commands

def run(num):
    for client_index in range(num):
        index = client_index % len(ip_list)
        ip = ip_list[index]
        print("run client on {} server in: {}, and read file {}.txt".format(client_index, ip, client_index))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip, username=user, password=password)
        p1 = "postgresql://naili:@{}:{}/cs5424db".format(ip.split(".")[0], server_port)
        p2 = "/home/stuproj/cs4224p/temp/tasks/project_files_4/xact_files_A/{}.txt".format(client_index)
        # command = f"pwd; cd temp/tasks; pwd"
        command = \
            f"cd temp/tasks; python3 cockroachDB_driver.py -u {p1} -p {p2} -w A " + \
            f">logs/python{client_index}.log &"
        stdin, stdout, stderr = client.exec_command(command)
        clients.append(client)
        stdouts.append(stdout)
        print(stderr.read())

    # Wait for commands to complete
    for i in range(len(stdouts)):
        print(stdouts[i].read())
        print(clients[i].close())

def kill():
    for ip in ip_list:
        print("Open session in: " + ip + "...")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip, username=user, password=password)
        command = \
            "ps -ef | grep  cockroachDB_driver.py | grep -v grep | awk  '{print $2}' | xargs  kill -9"
        stdin, stdout, stderr = client.exec_command(command)
        clients.append(client)
        stdouts.append(stdout)
        print(stderr.read())

    # Wait for commands to complete
    for i in range(len(stdouts)):
        print(stdouts[i].read())
        print(clients[i].close())


def check():
    for ip in ip_list:
        print("checking server: {} =======================>".format(ip))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip, username=user, password=password)
        command = "ps aux | grep cock"
        stdin, stdout, stderr = client.exec_command(command)
        clients.append(client)
        stdouts.append(stdout)
        if stderr.read():
            print(stderr.read())
        print(stdout.read().decode("utf-8"))


if __name__ == "__main__":
    # run(40)
    # kill()
    check()

