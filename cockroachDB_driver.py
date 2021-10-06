#!/usr/bin/env python3

import time
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
import psycopg2
from psycopg2.extras import LoggingConnection, LoggingCursor
import txs
from txs.base import Transactions
from txs.params import *
from txs.tx_workloadA import TxForWorkloadA
from txs.tx_workloadB import TxForWorkloadB
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# time spent running all transactions
total_tx_time = 0
# total number of transactions
total_tx_num = 0
# time spent running each transaction
each_tx_time = 0
# tx time list
time_used_list = []


# MyLoggingCursor simply sets self.timestamp at start of each query
class MyLoggingCursor(LoggingCursor):
    def execute(self, query, vars=None):
        self.timestamp = time.time()
        return super(MyLoggingCursor, self).execute(query, vars)

    def callproc(self, procname, vars=None):
        self.timestamp = time.time()
        return super(MyLoggingCursor, self).callproc(procname, vars)


# MyLogging Connection:
#   a) calls MyLoggingCursor rather than the default
#   b) adds resulting execution (+ transport) time via filter()
class MyLoggingConnection(LoggingConnection):
    def filter(self, msg, curs):
        global total_tx_time
        global each_tx_time
        time_used = int((time.time() - curs.timestamp) * 1000)
        total_tx_time += time_used
        each_tx_time += time_used
        return "[" + str(msg)[2:-1] + "]:   %d ms" % int((time.time() - curs.timestamp) * 1000)

    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', MyLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)


def test_tx(tx_ins: Transactions, m_conn):
    global total_tx_time
    global total_tx_num
    global time_used_list
    global each_tx_time
    tx_ins.test_transaction(m_conn)


def execute_tx(tx_ins: Transactions, m_conn, m_params):
    global total_tx_time
    global total_tx_num
    global time_used_list
    global each_tx_time

    total_tx_num += 1
    is_success = True
    try:

        if params.__class__.__name__ == txs.NewOrderTxName:
            tx_ins.new_order_transaction(m_conn, m_params)
        elif params.__class__.__name__ == txs.PaymentTxName:
            tx_ins.payment_transaction(m_conn, m_params)
        elif params.__class__.__name__ == txs.DeliveryTxName:
            tx_ins.delivery_transaction(m_conn, m_params)
        elif params.__class__.__name__ == txs.OrderStatusTxName:
            tx_ins.order_status_transaction(m_conn, m_params)
        elif params.__class__.__name__ == txs.StockLevelTxName:
            tx_ins.stock_level_transaction(m_conn, m_params)
        elif params is None:
            tx_ins.top_balance_transaction(m_conn)
        elif params.__class__.__name__ == txs.PopItemTxName:
            tx_ins.popular_item_transaction(m_conn, m_params)
        elif params.__class__.__name__ == txs.RelCustomerTxName:
            tx_ins.related_customer_transaction(m_conn, m_params)

        # The function below is used to test the transaction retry logic.  It
        # can be deleted from production code.
        # run_transaction(conn, test_retry_loop)
    except Exception as e:
        is_success = False
        raise

    if is_success:
        # record time used
        time_used_list.append(each_tx_time)
        each_tx_time = 0
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

    print("------------time spent list", time_used_list)
    print("------------Total time spent in tx", total_tx_time, "ms or", total_tx_time / 1000, "s", "------", )
    print("------------Throughout is", total_tx_num / (total_tx_time / 1000), " tx/s ------", )
    print("------------Average transaction latency (in ms) is", total_tx_time / total_tx_num, "ms------", )
    print("------------Median transaction latency (in ms) is", time_used_list[int(len(time_used_list)/2)], "ms------")


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
    file_path = "/opt/project_files/xact_files_A/0.txt"
    # workload_type = "A"

    TestTxConfig = False
    DebugSingleTx = False
    SingleTxName = txs.NewOrderTxName
    # if debug single transaction, assign name here

    conn = psycopg2.connect(dsn=addr, connection_factory=MyLoggingConnection)
    conn.initialize(logger)

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

    # read from file
    inputs = []
    f = open(file_path)
    line_content = f.readline()
    while line_content.strip():
        inputs.append(line_content.strip())
        triggered, params = parse_stdin(inputs)
        if TestTxConfig == True:
            test_tx(tx_ins, conn)
        if triggered:
            print("the triggered tx is ", params.__class__.__name__)
            # test only one tx
            if DebugSingleTx == True and params.__class__.__name__ != SingleTxName: inputs = [];  line_content = f.readline(); continue
            execute_tx(tx_ins, conn, params)
            inputs = []
        line_content = f.readline()

        if total_tx_num > 20:
            break

    f.close()
    evaluate()
    conn.close()


    # read from stdin
    # for user_input in sys.stdin:
    #     if user_input.strip() == "":
    #         continue
    #     inputs.append(user_input.strip())
    #     triggered, params = parse_stdin(inputs)
    #     if triggered:
    #         execute_tx(conn, params)
    #         inputs = []


