"""
    As orderline.csv is too large to denormalise with sql, this module denormalises it.
"""


import pandas as pd
import argparse, pathlib

ap = argparse.ArgumentParser()
ap.add_argument( '-f', '--file_path', help="Input file path, point to where project_files is, default is .", type=pathlib.Path, default=pathlib.Path('.'))
args = ap.parse_args()

data_paths = [args.file_path / 'project_files/data_files_A', args.file_path / 'project_files/data_files_B']

config = {
    'orders': ['order.csv', ['o_w_id', 'o_d_id', 'o_id', 'o_c_id', 'o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d']],
    'orderline': ['order-line.csv', ['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_number', 'ol_i_id', 'ol_delivery_d', 'ol_amount', 'ol_supply_w_id', 'ol_quantity', 'ol_dist_info']],
    'item': ['item.csv', ['i_id', 'i_name', 'i_price', 'i_im_id', 'i_data']],
   }

derived_config = {
    'orderline_denormalised': 'orderline_denormalised.csv',
}

for path in data_paths:
    print("Processing data on: " + str(path))

    table_list = {}

    for name, [file, cols] in config.items():
        table_list[name] = pd.read_csv(path / file, header=None, names=cols)


    # add ol_c_id and ol_i_name to orderline
    table_list['orderline_denormalised'] = pd.merge(table_list['orderline'], table_list['item'][['i_id','i_name']], left_on='ol_i_id', right_on='i_id', how='left').drop(columns=['i_id'])
    table_list['orderline_denormalised'] = table_list['orderline_denormalised'].merge(table_list['orders'][['o_w_id', 'o_d_id', 'o_id', 'o_c_id']], left_on=['ol_w_id', 'ol_d_id', 'ol_o_id'], right_on=['o_w_id', 'o_d_id','o_id'], how='left').drop(columns=['o_w_id', 'o_d_id','o_id'])

    # Export as csv
    for name, file in derived_config.items():
        table_list[name].to_csv(path / file, index=False, header=False)

print("Data processing done!")