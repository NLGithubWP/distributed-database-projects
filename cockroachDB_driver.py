#!/usr/bin/env python3

import time
import random
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
import psycopg2
from psycopg2.extras import LoggingConnection, LoggingCursor
import sys


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

update_batch_size = 100
select_batch_size = 1000

# time spent running all transactions
total_tx_time = 0
# total number of transactions
total_tx_num = 0
# time spent running each transaction
each_tx_time = 0
# tx time list
time_used_list = []

NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"
PopItemTxName = "PopItemTxParams"
# NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"


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

class NewOrderTxParams:
    def __init__(self):
        self.c_id: int = 0
        self.w_id: int = 0
        self.d_id: int = 0
        self.num_items: int = 0
        self.item_number: [int] = []
        self.supplier_warehouse: [int] = []
        self.quantity: [int] = []

class PopItemTxParams:
    def __init__(self):
        self.w_id: int = 0
        self.d_id: int = 0
        self.l: int = 0

def new_order_transaction(m_conn, m_params: NewOrderTxParams):
    """
    New Order Transaction consists of M+1 lines, where M denote the number of items in the new order.
    The first line consists of five comma-separated values: N, C_ID, W_ID, D_ID, M.
    Each of the M remaining lines specifies an item in the order and consists of
    three comma-separated values: OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY.
    eg:
        N,1144,1,1,7
        34981,1,1
        77989,1,7
        73381,1,5
        28351,1,1
        40577,1,4
        89822,1,4
        57015,1,2
    """
    c_id = m_params.c_id
    w_id = m_params.w_id
    d_id = m_params.d_id
    num_items = m_params.num_items
    item_number = m_params.item_number
    supplier_warehouse = m_params.supplier_warehouse
    quantity = m_params.quantity

    with m_conn.cursor() as cur:

        # 1. Let N denote value of the next available order number D NEXT O ID for district (W ID,D ID)
        cur.execute("SELECT D_NEXT_O_ID, D_TAX FROM district WHERE D_W_ID = %s and D_ID = %s", (w_id, d_id))
        res = cur.fetchone()
        n = res[0]
        d_tax = res[1]

        # 2. Update the district (W ID, D ID) by incrementing D NEXT O ID by one
        cur.execute("UPDATE district SET D_NEXT_O_ID = %s WHERE D_W_ID = %s and D_ID = %s", (n + 1, w_id, d_id))

        # 3. Create a new order record
        all_local = 1
        for i in range(1, num_items, 1):
            if supplier_warehouse[i] != w_id:
                all_local = 0
                break

        cur.execute("INSERT INTO order_ori (O_W_ID, O_D_ID, O_ID, O_C_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) "
                    "VALUES (%s,%s,%s,%s,%s,%s, now()) RETURNING O_ENTRY_D",
                    (w_id, d_id, n, c_id, num_items, all_local))
        entry_d = cur.fetchone()[0]

        # 4. Initialize TOTAL AMOUNT = 0
        total_amount = 0

        # 5. for i = 1 to num_items
        i_name_list = []
        o_amount_list = []
        s_quantity_list = []
        s_dist_xx = "S_DIST_" + str(d_id)

        for i in range(0, len(item_number)):

            cur.execute("SELECT S_QUANTITY, %s FROM stock WHERE S_W_ID = %s and S_I_ID = %s",
                        (s_dist_xx, supplier_warehouse[i], item_number[i]))
            res = cur.fetchone()
            s_quantity = res[0]
            s_dist_xx_value = res[1]

            adjusted_qty = s_quantity - quantity[i]

            if adjusted_qty < 10:
                adjusted_qty = adjusted_qty + 100

            if supplier_warehouse[i] != w_id:
                cur.execute(
                    "UPDATE stock SET S_QUANTITY = %s, S_YTD = %s,S_ORDER_CNT=S_ORDER_CNT+1 "
                    "WHERE S_W_ID = %s and S_I_ID = %s",
                    (adjusted_qty, quantity[i], supplier_warehouse[i], item_number[i]))
            else:
                cur.execute(
                    "UPDATE stock SET S_QUANTITY = %s,S_YTD = %s,S_ORDER_CNT=S_ORDER_CNT+1,S_REMOTE_CNT=S_REMOTE_CNT+1 "
                    "WHERE S_W_ID = %s and S_I_ID = %s",
                    (adjusted_qty, quantity[i], supplier_warehouse[i], item_number[i]))

            cur.execute("SELECT I_PRICE, I_NAME FROM item WHERE I_ID = %s", [item_number[i]])
            res = cur.fetchone()
            i_price = res[0]
            i_name = res[1]
            i_name_list.append(i_name)

            item_amount = quantity[i] * i_price
            total_amount += item_amount

            cur.execute(
                "INSERT INTO order_line "
                "(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) "
                "values (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (w_id, d_id, n, i, item_number[i], item_amount, supplier_warehouse[i], quantity[i], s_dist_xx))

            o_amount_list.append(item_amount)
            s_quantity_list.append(quantity[i])

        # 6. update all
        cur.execute("SELECT W_TAX FROM warehouse WHERE W_ID = %s", [w_id])
        w_tax = cur.fetchone()[0]

        cur.execute("SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM customer WHERE C_W_ID = %s and C_D_ID = %s and C_ID = %s",
                    (w_id, d_id, c_id))
        res = cur.fetchone()
        c_discount = res[0]
        c_last = res[1]
        c_credit = res[2]

        total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)
    m_conn.commit()

    print("------------ result is ---------")
    print("Customer identifier (W ID, D ID, C ID), lastname C_LAST, credit C_CREDIT, discount C DISCOUNT")
    print(w_id, d_id, c_id, c_last, c_credit, c_discount)
    print("Warehouse tax rate W TAX, District tax rate D TAX")
    print(w_tax, d_tax)
    print("Order number O ID, entry date O ENTRY D")
    print(n, entry_d)
    print("Number of items NUM_ITEMS, Total amount for order TOTAL_AMOUNT")
    print(num_items, total_amount)

    for i in range(0, len(item_number)):
        print("ITEM NUMBER[i]: ", item_number[i])
        print("I_NAME: ", i_name_list[i])
        print("SUPPLIER_WAREHOUSE[i]: ", supplier_warehouse[i])
        print("QUANTITY[i]: ", quantity[i])
        print("OL_AMOUNT: ", o_amount_list[i])
        print("S_QUANTITY: ", s_quantity_list[i])


def tx2():
    pass


def tx3():
    pass


def tx4():
    pass


def tx5():
    pass


def popular_item_transaction(m_conn, m_params: PopItemTxParams):
    print("tx6 triggered")
    w_id = m_params.w_id
    d_id = m_params.d_id
    l = m_params.l

    with m_conn.cursor() as cur:
        # 1. Let N denote value of the next available order number D NEXT O ID for district (W ID,D ID)
        cur.execute("SELECT D_NEXT_O_ID FROM district WHERE D_W_ID = %s AND D_ID = %s", (w_id, d_id))
        res = cur.fetchone()
        n = res[0]

        #2. Let S denote the set of last L orders for district (W ID,D ID)
        cur.execute("SELECT O_ID, O_ENTRY_D, C_FIRST, customer.C_MIDDLE, customer.C_LAST\
         FROM order_ori JOIN customer ON order_ori.O_W_ID = customer.C_W_ID AND order_ori.O_C_ID = customer.C_ID\
         WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID >= %s AND O_ID < %s",
                    (w_id, d_id, n-l, n))
        s = cur.fetchall()

        #3. For each order number x in S
        #Let Ix denote the set of order-lines for this order;
        # Let Px ⊆ Ix denote the subset of popular items in Ix
        for x in s:
            print("order: O_ID, O_ENTRY_ID")
            print(x[0], x[1])

            print("customer name")
            print(x[2], x[3], x[4])

            cur.execute(
            '''WITH order_line_items AS
            (SELECT item.I_NAME, order_line.OL_QUANTITY 
            FROM order_line JOIN item ON order_line.OL_I_ID = item.I_ID
            WHERE OL_O_ID = %s AND OL_D_ID = %s AND OL_W_ID = %s)
            SELECT I_NAME, OL_QUANTITY
            FROM order_line_items
            WHERE OL_QUANTITY = (SELECT MAX(OL_QUANTITY) FROM order_line_items);
            ''', (x[0], d_id, w_id))
            p_x = cur.fetchall()

            print("popular item: I_NAME, OL_QUANTITY")
            for item in p_x:
                print(item)

        #TODO: reuqirement 4, optimaise 3
        m_conn.commit()



def top_balance_transaction(m_conn):
    with m_conn.cursor() as cur:
        cur.execute(
            '''WITH top_customers AS
            (SELECT c_first, c_middle, c_last, c_balance, c_d_id, c_w_id FROM customer
            ORDER BY customer.c_balance DESC LIMIT 10)
            SELECT c_first, c_middle, c_last, c_balance, w_name, d_name
            FROM top_customers 
            JOIN district ON d_id=top_customers.c_d_id AND d_w_id=top_customers.c_w_id
            JOIN warehouse ON w_id=top_customers.c_w_id
            ORDER BY top_customers.c_balance DESC;
        ''')
        rows = cur.fetchall()
        m_conn.commit()

        print("------------ result is ---------")
        print("Top 10 customers ranked in descending order of outstanding balance:")
        print("C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, W_NAME, D_NAME")
        for row in rows:
            print(row)


def tx8():
    pass


def execute_tx(m_conn, m_params):
    global total_tx_time
    global total_tx_num
    global time_used_list
    global each_tx_time

    total_tx_num += 1
    is_success = True
    try:

        if params.__class__.__name__ == NewOrderTxName:
            new_order_transaction(m_conn, m_params)
        elif params is None:
            top_balance_transaction(m_conn)
        elif params.__class__.__name__ == PopItemTxName:
            print("param type is detected")
            popular_item_transaction(m_conn, m_params)


        # The function below is used to test the transaction retry logic.  It
        # can be deleted from production code.
        # run_transaction(conn, test_retry_loop)
    except ValueError as ve:
        is_success = False
        # Below, we print the error and continue on so this example is easy to
        # run (and run, and run...).  In real code you should handle this error
        # and any others thrown by the database interaction.
        logging.debug("run_transaction(conn, op) failed: %s", ve)
        pass
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
        return False, "not-implemented"
    elif tmp_list[0] == "D":
        return False, "not-implemented"
    elif tmp_list[0] == "O":
        return False, "not-implemented"
    elif tmp_list[0] == "S":
        return False, "not-implemented"
    elif tmp_list[0] == "I":
        print("I detected")
        m_params = PopItemTxParams()
        m_params.w_id = int(tmp_list[1])
        m_params.d_id = int(tmp_list[2])
        m_params.l = int(tmp_list[3])
        return True, m_params
    elif tmp_list[0] == "T":
        return True, None
    elif tmp_list[0] == "R":
        return False, "not-implemented"
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
    print("------------Throughout is", total_tx_num / (total_tx_time / 1000), "------", )
    print("------------Average transaction latency (in ms) is", total_tx_time / total_tx_num, "ms------", )
    print("------------Median transaction latency (in ms) is", time_used_list[int(len(time_used_list)/2)], "ms------")


# python3 cockroachDB_driver.py "postgresql://naili:naili@localhost:26257/cs5424?sslmode=require"
if __name__ == "__main__":

    isDebug = True
    addr = "postgresql://naili:naili@localhost:26257/cs5424?sslmode=require"
    conn = psycopg2.connect(dsn=addr, connection_factory=MyLoggingConnection)
    conn.initialize(logger)

    inputs = []

    if isDebug:
        # read from file
        f = open(
            "/Users/nailixing/Documents/NUS_Modules/CS5424_Distributed_Database/projects/project_files/xact_files_A/0.txt")
        line_content = f.readline()
        while line_content.strip():
            inputs.append(line_content.strip())
            triggered, params = parse_stdin(inputs)
            if triggered:
                execute_tx(conn, params)
                inputs = []
            elif params == "not-implemented":
                inputs = []
            line_content = f.readline()

            if total_tx_num > 2:
                break

        f.close()
        evaluate()
        conn.close()
        exit(0)

    # read from stdin
    for user_input in sys.stdin:
        if user_input.strip() == "":
            continue
        inputs.append(user_input.strip())
        triggered, params = parse_stdin(inputs)
        if triggered:
            execute_tx(conn, params)
            inputs = []

    conn.close()
