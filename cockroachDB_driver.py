#!/usr/bin/env python3

import time
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
import psycopg2
import csv
import pandas as pd
from psycopg2.extras import LoggingConnection, LoggingCursor
import txs
from txs.base import Transactions
from txs.params import *
from txs.tx_workloadA import TxForWorkloadA
from txs.tx_workloadB import TxForWorkloadB
logging.basicConfig(filename='./txs.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)

# time spent running all transactions
total_tx_time = 0
# total number of transactions
total_tx_num = 0
# tx time list
time_used_list = []
max_retry_time = 5

class MyLoggingCursor(LoggingCursor):
    def execute(self, query, vars=None):
        self.timestamp = time.time()
        return super(MyLoggingCursor, self).execute(query, vars)

    def callproc(self, procname, vars=None):
        self.timestamp = time.time()
        return super(MyLoggingCursor, self).callproc(procname, vars)


class MyLoggingConnection(LoggingConnection):
    def filter(self, msg, curs):
        time_used = int((time.time() - curs.timestamp) * 1000000)  # us
        return "[" + str(msg)[2:-1] + "]:   %d us" % time_used # us

    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', MyLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)


def test_tx(tx_ins: Transactions, m_conn):
    global total_tx_time
    global total_tx_num
    global time_used_list
    tx_ins.test_transaction(m_conn)


def execute_tx(tx_ins: Transactions, m_conn, m_params):
    global total_tx_time
    global total_tx_num
    global time_used_list
    global max_retry_time

    total_tx_num += 1
    is_success = True
    each_tx_time = 0

    run_time = 0
    while True:
        run_time += 1
        if run_time > max_retry_time:
            raise
        try:

            if params.__class__.__name__ == txs.NewOrderTxName:
                each_tx_time = tx_ins.new_order_transaction(m_conn, m_params)
            elif params.__class__.__name__ == txs.PaymentTxName:
                each_tx_time = tx_ins.payment_transaction(m_conn, m_params)
            elif params.__class__.__name__ == txs.DeliveryTxName:
                each_tx_time = tx_ins.delivery_transaction(m_conn, m_params)
            elif params.__class__.__name__ == txs.OrderStatusTxName:
                each_tx_time = tx_ins.order_status_transaction(m_conn, m_params)
            elif params.__class__.__name__ == txs.StockLevelTxName:
                each_tx_time = tx_ins.stock_level_transaction(m_conn, m_params)
            elif params is None:
                each_tx_time = tx_ins.top_balance_transaction(m_conn)
            elif params.__class__.__name__ == txs.PopItemTxName:
                each_tx_time = tx_ins.popular_item_transaction(m_conn, m_params)
            elif params.__class__.__name__ == txs.RelCustomerTxName:
                each_tx_time = tx_ins.related_customer_transaction(m_conn, m_params)
            break
        except Exception as e:
            is_success = False
            m_conn.rollback()
            if "retry" in e:
                time.sleep(0.01)
        except Exception as e:
            raise e

    if is_success:
        # record time used
        time_used_list.append(each_tx_time)
        total_tx_time += each_tx_time
    else:
        total_tx_num -= 1


def parse_stdin(m_inputs: [str]):
    """
    :param m_inputs: N, P, D, O, S, I, T, or R
    :return:
    """
    tmp_list = m_inputs[0].split(",")
    if tmp_list[0] == "N":
        if len(m_inputs) < int(tmp_list[4]) + 1:
            return False, None
        else:
            m_params = NewOrderTxParams()
            m_params.c_id = int(tmp_list[1])
            m_params.w_id = int(tmp_list[2])
            m_params.d_id = int(tmp_list[3])
            m_params.num_items = int(tmp_list[4])
            m_params.item_number = []
            m_params.supplier_warehouse = []
            m_params.quantity = []
            for line_id in range(1, len(m_inputs), 1):
                tmp_remain_line_list = m_inputs[line_id].split(",")
                m_params.item_number.append(int(tmp_remain_line_list[0]))
                m_params.supplier_warehouse.append(int(tmp_remain_line_list[1]))
                m_params.quantity.append(int(tmp_remain_line_list[2]))

            return True, m_params
    elif tmp_list[0] == "P":
        # eg: P,1,1,2203,3261.87
        m_params = PaymentTxParams()
        m_params.c_w_id = int(tmp_list[1])
        m_params.c_d_id = int(tmp_list[2])
        m_params.c_id = int(tmp_list[3])
        m_params.payment_amount = float(tmp_list[4])

        return True, m_params

    elif tmp_list[0] == "D":
        # D,1,6
        m_params = DeliveryTxParams()
        m_params.w_id = int(tmp_list[1])
        m_params.carrier_id = int(tmp_list[2])
        return True, m_params

    elif tmp_list[0] == "O":
        # O,1,1,1219
        # o,1,1,1144
        m_params = OrderStatusTxParams()
        m_params.c_w_id = int(tmp_list[1])
        m_params.c_d_id = int(tmp_list[2])
        m_params.c_id = int(tmp_list[2])
        return True, m_params

    elif tmp_list[0] == "S":
        m_params = StockLevelTxParams()
        m_params.w_id = int(tmp_list[1])
        m_params.d_id = int(tmp_list[2])
        m_params.threshold = (tmp_list[3])
        m_params.l = int(tmp_list[4])
        return True, m_params

    elif tmp_list[0] == "I":
        m_params = PopItemTxParams()
        m_params.w_id = int(tmp_list[1])
        m_params.d_id = int(tmp_list[2])
        m_params.l = int(tmp_list[3])
        return True, m_params

    elif tmp_list[0] == "T":
        return True, None

    elif tmp_list[0] == "R":
        m_params = RelCustomerTxParams()
        m_params.w_id = int(tmp_list[1])
        m_params.d_id = int(tmp_list[2])
        m_params.c_id = int(tmp_list[3])
        return True, m_params

    else:
        return False, None


def evaluate():
    """
    • Total number of transactions processed
    • Total elapsed time for processing the transactions (in seconds)
    • Transaction throughput (number of transactions processed per second)
    • Average transaction latency (in ms)
    • Median transaction latency (in ms)
    • 95th percentile transaction latency (in ms)
    • 99th percentile transaction latency (in ms)
    :return:
    """
    global total_tx_time
    global total_tx_num

    time_used_list.sort()
    time_df = pd.Series(time_used_list)

    # append performance metrics to client.csv
    output = {}
    output['client_number'] = file_path.split('/')[-1].replace('.txt', '')
    output['num_xacts'] = total_tx_num
    output['total_elapsed_time'] = total_tx_time # in s
    output['xact_throughput'] = total_tx_num / total_tx_time
    output['avg_xact_latency'] = time_df.mean() * 1000  # in ms
    output['median_xact_latency'] = time_df.median() * 1000  # in ms
    output['95_pct_xact_latency'] = time_df.quantile(0.95) * 1000  # in ms
    output['99_pct_xact_latency'] = time_df.quantile(0.99) * 1000  # in ms

    df = pd.DataFrame([output])
    df.to_csv('output/clients.csv', mode='a', header=False, index=False)


def parse_cmdline():
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawTextHelpFormatter)

    parser.add_argument("-u","--url", help="database url")
    parser.add_argument("-p", "--path", help="path of workload files")
    parser.add_argument("-w", "--workload_type", help="workload type, A or B")

    opt = parser.parse_args()
    return opt


if __name__ == "__main__":

    # batch used to insert or select
    update_batch_size = 100
    select_batch_size = 100

    # Schema names
    workloadA_schema_name = "workloada"
    workloadB_schema_name = "workloadb"

    # parser result
    opt = parse_cmdline()
    addr = opt.url
    file_path = opt.path
    workload_type = opt.workload_type

    # addr = "postgresql://naili:naili@localhost:26257/cs5424db?sslmode=require"
    # file_path = "/mnt/c/a.SCHOOL/Master/distributed_database/tasks/project_files/xact_files_B/0.txt"
    # workload_type = "A"


    TestTxConfig = False
    DebugSingleTx = False
    SingleTxName = txs.TopBalanceTxName
    # if debug single transaction, assign name here

    conn = psycopg2.connect(dsn=addr, connection_factory=MyLoggingConnection)
    # conn = psycopg2.connect(dsn=addr)
    conn.initialize(logger)

    tx_types = {}

    # choose workload
    tx_ins = None
    if workload_type == "A":
        with conn.cursor() as cur:
            cur.execute("set search_path to "+workloadA_schema_name)
            conn.commit()
        tx_ins = TxForWorkloadA(update_batch_size, select_batch_size)
    elif workload_type == "B":
        with conn.cursor() as cur:
            cur.execute("set search_path to "+workloadB_schema_name)
            conn.commit()
        tx_ins = TxForWorkloadB(update_batch_size, select_batch_size)
    else:
        exit(0)

    # read from a single xact file
    inputs = []
    f = open(file_path)
    line_content = f.readline()
    while line_content.strip():
        inputs.append(line_content.strip())
        triggered, params = parse_stdin(inputs)
        if TestTxConfig == True:
            test_tx(tx_ins, conn)
        if triggered:
            if params.__class__.__name__ not in tx_types:
                tx_types[params.__class__.__name__] = 1
            else:
                tx_types[params.__class__.__name__] += 1

            logger.info("the triggered tx is " + params.__class__.__name__)
            # test only one tx
            if DebugSingleTx == True and params.__class__.__name__ != SingleTxName: inputs = []; line_content = f.readline(); continue
            execute_tx(tx_ins, conn, params)
            inputs = []
            if total_tx_num > 7:
                break
        line_content = f.readline()

    f.close()
    logger.info("============================using file " +file_path + "=====================")
    logger.info(tx_types)
    conn.close()
    evaluate()





