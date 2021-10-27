import pandas as pd

client_col = ['client_number','num_xacts','total_elapsed_time','xact_throughput',
              'avg_xact_latency','median_xact_latency','95_pct_xact_latency','99_pct_xact_latency']

throughput_col = ['min_throughput','max_throughput','avg_throughput']

dbstate_col = ['sum_w_ytd','sum_d_ytd','sum_d_next_o_id','sum_c_balance','sum_c_ytd_payment','sum_c_payment_cnt','sum_c_delivery_cnt','max_o_id','sum_o_ol_cnt',
              'sum_ol_amount','sum_ol_quantity','sum_s_quantity','sum_s_ytd','sum_s_order_cnt','sum_s_remote_cnt']


df_client = pd.DataFrame([],columns=client_col)
df_throughput = pd.DataFrame([],columns=throughput_col)
df_dbstate = pd.DataFrame([],columns=dbstate_col)

df_client.to_csv('output/clients.csv', index=False)
df_throughput.to_csv('output/throughput.csv', index=False)
df_dbstate.to_csv('output/dbstate.csv', index=False)