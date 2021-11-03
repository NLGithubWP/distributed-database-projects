#!/usr/bin/env python3

import time
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
import random

import psycopg2
import csv
import traceback
import pandas as pd
from psycopg2.extras import LoggingConnection, LoggingCursor
import txs
from txs.base import Transactions
from txs.params import *
from txs.tx_workloadA import TxForWorkloadA
from txs.tx_workloadB import TxForWorkloadB

# time spent running all transactions, in us
total_tx_time = 0
# total number of transactions
total_tx_num = 0
# tx time list, in us
time_used_list = []
max_retry_time = 5
RETRYERRORMSG = "restart transaction"
required_tx_types = {}
succeed_tx_types = {}
tx_time_range = {}
tx_times = {}


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
        return "[" + " ".join([ele.strip() for ele in msg.decode("utf-8").split("\n")]) + "]:   %d us" % time_used  # us

    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', MyLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)


def test_tx(tx_ins: Transactions, m_conn):
    global total_tx_time
    global total_tx_num
    global time_used_list
    tx_ins.test_transaction(m_conn)


def run_tx(m_conn, tx_name, operation):
    with m_conn:
        for try_time in range(1, max_retry_time + 1):
            try:
                res = operation(m_conn)
                if try_time > 1:
                    logger.info("Running Tx {} successful at the {} time retry".
                                format(tx_name, try_time))

                # if successful, return
                return True, res
            except Exception as e:
                m_conn.rollback()
                # if it's retry error, retry it
                if RETRYERRORMSG in str(e):
                    logger.error("Errored: {} retry happened in running tx: ".format(
                        try_time) + tx_name +
                                 ", ErrorMsg: \n[ {} ]".format(str(e)) +
                                 ", Traceback: \n[ {} ]".format(traceback.format_exc()))
                    sleep_ms = (2 ** try_time) * 0.1 * (random.random() + 0.5)
                    time.sleep(sleep_ms)
                # otherwise, log the error and return
                else:
                    logger.error("Errored: Unknown Error in running tx: " + tx_name +
                                 ", ErrorMsg: \n[ {} ]".format(str(e)) +
                                 ", Traceback: \n[ {} ]".format(traceback.format_exc()))
                    return False, -1
        logger.error("Errored: Max Try time reached, tx {} still error! ".format(tx_name))
        return False, -1


def execute_tx(m_tx_ins: Transactions, m_conn, m_params):
    global total_tx_time
    global total_tx_num
    global time_used_list
    global max_retry_time
    global succeed_tx_types

    tx_name = m_params.__class__.__name__
    status = False
    each_tx_time = 0

    if tx_name == txs.NewOrderTxName:
        status, each_tx_time = run_tx(m_conn, tx_name,
                                      lambda l_conn: m_tx_ins.new_order_transaction(l_conn, m_params))

    elif tx_name == txs.PaymentTxName:
        status, each_tx_time = run_tx(m_conn, tx_name,
                                      lambda l_conn: m_tx_ins.payment_transaction(l_conn, m_params))

    elif tx_name == txs.DeliveryTxName:
        # =============================================> for each d_id, run a tx to update it. avoid tx congestion
        # for d_id in range(1, 11, 1):
        #     tmp_status, tmp_tx_time = run_tx(m_conn, tx_name+"-"+str(d_id),
        #                          lambda l_conn: m_tx_ins.delivery_transaction_one_did(l_conn, m_params, d_id))
        #
        #     # once one tx errored, record as error and skip the rest
        #     status = tmp_status
        #     if not tmp_status:
        #         break
        #     else:
        #         each_tx_time += tmp_tx_time

        # =============================================> once update all
        # status, each_tx_time = run_tx(m_conn, tx_name,
        #                               lambda l_conn: m_tx_ins.delivery_transaction(l_conn, m_params))

        # =============================================> read-update-(many_update)
        # tmp_status, res = run_tx(m_conn, tx_name + "-read",
        #                          lambda l_conn: m_tx_ins.delivery_read_transaction(l_conn, m_params))
        #
        # if not tmp_status:
        #     return
        #
        # tmp_tx_time, w_id, carrier_id, id_tuples, sum_map = res
        # each_tx_time += tmp_tx_time
        #
        # tmp_status, res = run_tx(m_conn, tx_name + "-update1",
        #                          lambda l_conn: m_tx_ins.delivery_update_transaction1(
        #                              l_conn, w_id, carrier_id, id_tuples))
        # if not tmp_status:
        #     return
        #
        # tmp_tx_time, cid_map = res
        # each_tx_time += tmp_tx_time
        #
        # for ele, sum_value in sum_map.items():
        #     c_id = cid_map[ele]
        #     tmp_status, tmp_tx_time = run_tx(m_conn, tx_name + "-update2",
        #                                      lambda l_conn: m_tx_ins.delivery_update_transaction2(
        #                                          l_conn, c_id, ele, sum_value))
        #     if not tmp_status:
        #         return
        #     each_tx_time += tmp_tx_time
        #
        # status = True

        # =============================================> read-update
        # tmp_status, res = run_tx(m_conn, tx_name + "-read",
        #                          lambda l_conn: m_tx_ins.delivery_read_transaction(l_conn, m_params))
        #
        # if not tmp_status:
        #     return
        #
        # tmp_tx_time, w_id, carrier_id, id_tuples, sum_map = res
        # each_tx_time += tmp_tx_time
        #
        # tmp_status, tmp_tx_time = run_tx(m_conn, tx_name + "-update3",
        #                                  lambda l_conn: m_tx_ins.delivery_update_transaction3(
        #                                      l_conn, w_id, carrier_id, id_tuples, sum_map))
        # if not tmp_status:
        #     return
        #
        # each_tx_time += tmp_tx_time
        #
        # status = True

        # =============================================> read-update-(one_update)
        tmp_status, res = run_tx(m_conn, tx_name + "-read",
                                 lambda l_conn: m_tx_ins.delivery_read_transaction(l_conn, m_params))

        if not tmp_status:
            return

        tmp_tx_time1, w_id, carrier_id, id_tuples, sum_map = res

        tmp_status, res = run_tx(m_conn, tx_name + "-update1",
                                 lambda l_conn: m_tx_ins.delivery_update_transaction1(
                                     l_conn, w_id, carrier_id, id_tuples))
        if not tmp_status:
            return

        tmp_tx_time2, cid_map = res

        tmp_status, tmp_tx_time3 = run_tx(m_conn, tx_name + "-update4",
                                         lambda l_conn: m_tx_ins.delivery_update_transaction4(
                                             l_conn, cid_map, sum_map))
        if not tmp_status:
            return

        each_tx_time = tmp_tx_time1+tmp_tx_time2+tmp_tx_time3
        status = True



    elif tx_name == txs.OrderStatusTxName:
        status, each_tx_time = run_tx(m_conn, tx_name,
                                      lambda l_conn: m_tx_ins.order_status_transaction(l_conn, m_params))

    elif tx_name == txs.StockLevelTxName:
        status, each_tx_time = run_tx(m_conn, tx_name,
                                      lambda l_conn: m_tx_ins.stock_level_transaction(l_conn, m_params))

    elif tx_name == txs.TopBalanceTxName:
        status, each_tx_time = run_tx(m_conn, tx_name,
                                      lambda l_conn: m_tx_ins.top_balance_transaction(l_conn))

    elif tx_name == txs.PopItemTxName:
        status, each_tx_time = run_tx(m_conn, tx_name,
                                      lambda l_conn: m_tx_ins.popular_item_transaction(l_conn, m_params))

    elif tx_name == txs.RelCustomerTxName:
        status, each_tx_time = run_tx(m_conn, tx_name,
                                      lambda l_conn: m_tx_ins.related_customer_transaction(l_conn, m_params))

    else:
        logger.error("Errored: txs Method not found, " + tx_name)

    # record the tx running status
    if status:
        # logger.info("======================================= running tx {} takes {} second"
        #             " ================================".
        #             format(tx_name, each_tx_time))

        time_used_list.append(each_tx_time * 1000000)
        total_tx_time += each_tx_time * 1000000
        total_tx_num += 1
        # update successful txs
        if tx_name not in succeed_tx_types:
            succeed_tx_types[tx_name] = 1
        else:
            succeed_tx_types[tx_name] += 1

        # update tx time intervals
        if tx_name not in tx_time_range:
            tx_time_range[tx_name] = [each_tx_time, each_tx_time]
        else:
            if each_tx_time > tx_time_range[tx_name][1]:
                tx_time_range[tx_name][1] = each_tx_time
            if each_tx_time < tx_time_range[tx_name][0]:
                tx_time_range[tx_name][0] = each_tx_time
        # record all time used
        if tx_name not in tx_times:
            tx_times[tx_name] = [each_tx_time]
        else:
            tx_times[tx_name].append(each_tx_time)


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
        m_params = TopBalanceTxParams()
        return True, m_params

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
    output['total_elapsed_time'] = total_tx_time / 1000000  # in s
    output['xact_throughput'] = total_tx_num / (total_tx_time / 1000000)
    output['avg_xact_latency'] = time_df.mean() * 0.001  # in ms
    output['median_xact_latency'] = time_df.median() * 0.001  # in ms
    output['95_pct_xact_latency'] = time_df.quantile(0.95) * 0.001  # in ms
    output['99_pct_xact_latency'] = time_df.quantile(0.99) * 0.001  # in ms

    df = pd.DataFrame([output])
    df.to_csv('output/clients.csv', mode='a', header=False, index=False)


def parse_cmdline():
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawTextHelpFormatter)

    parser.add_argument("-u", "--url", help="database url")
    parser.add_argument("-p", "--path", help="path of workload files")
    parser.add_argument("-w", "--workload_type", help="workload type, A or B")

    opt = parser.parse_args()
    return opt


if __name__ == "__main__":

    begin_time = time.time()
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
    log_file_name = 'logs/tx_log_{}'.format(file_path.split("/")[-1])
    print("logging to file " + log_file_name)
    logging.basicConfig(filename=log_file_name, level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    TestTxConfig = False
    # if debug single transaction, set DebugSingleTx = true and assign name here
    DebugSingleTx = False
    SingleTxName = txs.RelCustomerTxName

    # Create a new database connection.

    # conn = psycopg2.connect(dsn=addr, connection_factory=MyLoggingConnection)
    # conn.initialize(logger)
    conn = psycopg2.connect(dsn=addr)

    # choose workload
    tx_ins = None
    if workload_type == "A":
        with conn.cursor() as cur:
            cur.execute("set search_path to " + workloadA_schema_name)
            conn.commit()
        tx_ins = TxForWorkloadA(update_batch_size, select_batch_size)
    elif workload_type == "B":
        with conn.cursor() as cur:
            cur.execute("set search_path to " + workloadB_schema_name)
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
            if params.__class__.__name__ not in required_tx_types:
                required_tx_types[params.__class__.__name__] = 1
            else:
                required_tx_types[params.__class__.__name__] += 1

            # test only one tx
            if DebugSingleTx == True and params.__class__.__name__ != SingleTxName: inputs = []; line_content = f.readline(); continue
            execute_tx(tx_ins, conn, params)
            # clear the inputs, and wait for next inout arrays
            inputs = []
            if total_tx_num > 1 and total_tx_num % 10 == 0:
                logger.info("============================ read file {}, now "
                            "total_tx_num reaches {}, write log to {} =====================".
                            format(file_path.split("/")[-1], total_tx_num, log_file_name))

            if total_tx_num > 110:
                break
        line_content = f.readline()

    f.close()
    end_time = time.time()
    total_time_used = end_time - begin_time
    logger.info("============================ using file " + file_path + " where total txs {}=====================".
                format(total_tx_num))

    for key in tx_times:
        tx_times[key].sort()

    infor = "connect to {}, Read file {} and run workload {}".format(addr, file_path, workload_type)
    logger.info(infor)
    logger.info("=======> required_tx_types is: ")
    logger.info(required_tx_types)
    logger.info("=======> succeed_tx_types is: ", succeed_tx_types)
    logger.info(succeed_tx_types)
    logger.info("=======> tx num percentages is: ", succeed_tx_types)
    logger.info({key: str(100 * value / total_tx_num)[:5] + "%" for key, value in succeed_tx_types.items()})
    logger.info("=======> tx_time_range [min-max] is : ", tx_time_range)
    logger.info(tx_time_range)
    logger.info("=======> tx_time middle time is : ")
    logger.info({key: value[len(value) // 2] for key, value in tx_times.items()})
    logger.info("=======> tx_time average time is : ")
    logger.info({key: sum(value) / len(value) for key, value in tx_times.items()})
    logger.info("=======> tx time percentage is : ")
    logger.info({key: str(100 * sum(value) / total_time_used)[:5] + "%" for key, value in tx_times.items()})
    logger.info("============> full tx time used ")
    logger.info({key: value for key, value in tx_times.items()})

    logger.info(
        "============================ total time used: {} second =====================".format(total_time_used))
    conn.close()
    evaluate()
