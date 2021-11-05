import pandas as pd
import psycopg2
from argparse import ArgumentParser, RawTextHelpFormatter

# only run this file after 40 clients' metrics are populated in clients.csv.

parser = ArgumentParser(description=__doc__,  formatter_class=RawTextHelpFormatter)
parser.add_argument("-u", "--url", help="database url")
parser.add_argument("-w", "--workload_type", help="workload type, A or B")
opt = parser.parse_args()

## Throughput.csv ##
throughput_col = ['min_throughput','max_throughput','avg_throughput']

df_clients = pd.read_csv('output/clients.csv')
df = pd.DataFrame([], columns = throughput_col)
df = df.append({'min_throughput' : df_clients['xact_throughput'].min(),
                'max_throughput' : df_clients['xact_throughput'].max(),
                'avg_throughput' : df_clients['xact_throughput'].mean()}, ignore_index=True)

df.to_csv('output/throughput.csv', header=True, index=False)
print('-- throughtput.csv value updated --')

## dbstate.csv ##
workloadA_schema_name = "workloada"
workloadB_schema_name = "workloadb"
addr = opt.url
workload_type = opt.workload_type
conn = psycopg2.connect(dsn=addr)
df = pd.DataFrame()
query_list = []

query_list.append('select sum(w_ytd) from warehouse')
query_list.append('select sum(d_ytd) , sum(d_next_o_id) from district')
query_list.append('select sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt) from customer')
query_list.append('select max(o_id), sum(o_ol_cnt) from order_ori')
query_list.append('select sum(ol_amount), sum(ol_quantity) from order_line')
query_list.append('select sum(s_quantity), sum(s_ytd),sum(s_order_cnt),sum(s_remote_cnt) from stock')

with conn.cursor() as cur:
    cur.execute("use cs5424db")
    if workload_type == "A":
        cur.execute("set search_path to "+workloadA_schema_name)
    else:
        cur.execute("set search_path to "+workloadB_schema_name)

    for q in query_list:
        cur.execute(q)
        value = cur.fetchone()
        for c in value:
            df = df.append([c])
conn.commit()

df.to_csv('output/dbstate.csv', header=False, index=False)
print('-- dbstate.csv value updated --')
