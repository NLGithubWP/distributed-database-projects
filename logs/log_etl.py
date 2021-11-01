


stat = {}
total_time = 0

res = []
mapper = {}


for id in range(0, 39):
    file_name = "/Users/nailixing/Documents/NUS_Modules/CS5424_Distributed_Database/projects/CS5424/logs/tx_log_"+str(id)
    print(file_name)
    with open(file_name+".txt", "r") as f:
        line_contents = f.readlines()
        for line_content in line_contents:
            if "retry happened in running tx: " in line_content:
                tx_name = line_content.strip().split(":")[-2].split(",")[0]
                if tx_name not in stat:
                    stat[tx_name] = 1
                else:
                    stat[tx_name] += 1

            if " total time used: " in line_content:
                total_time += float(line_content.split(" ")[-3])
                res.append(float(line_content.split(" ")[-3]))
                if file_name not in stat:
                    mapper[float(line_content.split(" ")[-3])] = [file_name]
                else:
                    mapper[float(line_content.split(" ")[-3])].append(file_name)


# 打印最快和最慢的client，然后人为地去看文件内的信息
res.sort()
print(mapper[res[0]])
print(mapper[res[-1]])
