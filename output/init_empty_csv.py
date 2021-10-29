import pandas as pd

# to create an empty client csv before running the driver.
# for drivers to append metrics to

client_col = ['client_number','num_xacts','total_elapsed_time','xact_throughput',
              'avg_xact_latency','median_xact_latency','95_pct_xact_latency','99_pct_xact_latency']

df_client = pd.DataFrame([],columns=client_col)
df_client.to_csv('output/clients.csv', index=False)