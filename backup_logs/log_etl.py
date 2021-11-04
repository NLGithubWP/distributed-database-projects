


stat = {}
total_time = 0

res = []
mapper = {}
fastest_slowest = {}

for id in range(20, 30):
    file_name = "logs/tx_log_"+str(id)
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

            if "now total_tx_num reaches" in line_content:
                if file_name not in fastest_slowest:
                    fastest_slowest[file_name] = 1
                else:
                    fastest_slowest[file_name] += 1

# 打印最快和最慢的client，然后人为地去看文件内的信息
if len(res) > 0:
    res.sort()
    print(mapper[res[0]])
    print(mapper[res[-1]])


print(sorted(fastest_slowest.items(), key=lambda kv:(kv[1], kv[0])))
