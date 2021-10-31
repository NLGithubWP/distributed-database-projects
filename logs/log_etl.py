


stat = {}
total_time = 0

for id in range(0, 39):
    file_name = "errorLogs/tx_log_"+str(id)
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


print(stat)
print("total-time: ", total_time)
print("throughput: ", 900*39/total_time)
print("average latency: ", total_time/(900*39))



'''
最新的
{' NewOrderTxParams': 49, ' PaymentTxParams': 59, ' DeliveryTxParams-1': 1, ' DeliveryTxParams-9': 1, ' StockLevelTxParams': 4, ' DeliveryTxParams-3': 1}
total-time:  96048.34614467621
throughput:  0.36544096185820196
average latency:  2.736420118081943
'''


'''
之前的
{' RelCustomerTxParams': 29, ' NewOrderTxParams': 69, ' PaymentTxParams': 49, ' DeliveryTxParams-5': 4, ' DeliveryTxParams-3': 2, ' StockLevelTxParams': 3, ' DeliveryTxParams-7': 2, ' DeliveryTxParams-2': 2, ' DeliveryTxParams-4': 1}
total-time:  87714.9020178318
throughput:  0.4001600548201539
average latency:  2.4990000574880855
'''
