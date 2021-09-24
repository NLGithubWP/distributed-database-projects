#!/usr/bin/env python3

import time
import random
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
import psycopg2
from psycopg2.errors import SerializationFailure
from psycopg2.extras import LoggingConnection, LoggingCursor

update_batch_size = 100
select_batch_size = 1000


NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"
# NewOrderTxName = "NewOrderTxParams"


class NewOrderTxParams:
    def __init__(self):
        self.c_id: int = 0
        self.w_id: int = 0
        self.d_id: int = 0
        self.num_items: int = 0
        self.item_number: [int] = []
        self.supplier_warehouse: [int] = []
        self.quantity: [int] = []


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
                    "VALUES (%s,%s,%s,%s,%s,%s, now()) RETURNING O_ENTRY_D", (w_id, d_id, n, c_id, num_items, all_local))
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
                    "UPDATE stock SET S_QUANTITY=%s,S_YTD=%s,S_ORDER_CNT=S_ORDER_CNT+1 WHERE S_W_ID = %s and S_I_ID = %s",
                    (adjusted_qty, quantity[i], supplier_warehouse[i], item_number[i]))
            else:
                cur.execute(
                    "UPDATE stock SET S_QUANTITY=%s,S_YTD=%s,S_ORDER_CNT=S_ORDER_CNT+1,S_REMOTE_CNT=S_REMOTE_CNT+1 WHERE S_W_ID = %s and S_I_ID = %s",
                    (adjusted_qty, quantity[i], supplier_warehouse[i], item_number[i]))

            cur.execute("SELECT I_PRICE, I_NAME FROM item WHERE I_ID = %s", [item_number[i]])
            res = cur.fetchone()
            i_price = res[0]
            i_name = res[1]
            i_name_list.append(i_name)

            item_amount = quantity[i] * i_price
            total_amount += item_amount

            cur.execute(
                "INSERT INTO order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) "
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
    print(cur.statusmessage)

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


def tx6():
    pass


def tx7():
    pass


def tx8():
    pass


def execute_tx(m_conn, m_params):
    try:

        if params.__class__.__name__ == NewOrderTxName:
            new_order_transaction(m_conn, m_params)

        # The function below is used to test the transaction retry logic.  It
        # can be deleted from production code.
        # run_transaction(conn, test_retry_loop)
    except ValueError as ve:
        # Below, we print the error and continue on so this example is easy to
        # run (and run, and run...).  In real code you should handle this error
        # and any others thrown by the database interaction.
        logging.debug("run_transaction(conn, op) failed: %s", ve)
        pass
    except Exception as e:
        raise


def parse_stdin(m_inputs: [str]):
    tmp_list = m_inputs[0].split(",")
    if tmp_list[0] == "N":
        if len(m_inputs) < int(tmp_list[4])+1:
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
    else:
        return False, _


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


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
        return "["+str(msg)[2:-1] + "]:   %d ms" % int((time.time() - curs.timestamp) * 1000)

    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', MyLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)


# python3 cockroachDB_driver.py "postgresql://naili:naili@localhost:26257/cs5424?sslmode=require"
if __name__ == "__main__":
    isDebug = True
    addr = "postgresql://naili:naili@localhost:26257/cs5424?sslmode=require"
    conn = psycopg2.connect(dsn=addr, connection_factory=MyLoggingConnection)
    conn.initialize(logger)

    if isDebug:
        inputs = ["N,1144,1,1,7",
                  "34981,1,1", "77989,1,7", "73381,1,5", "28351,1,1", "40577,1,4", "89822,1,4", "57015,1,2"]
        triggered, params = parse_stdin(inputs)
        execute_tx(conn, params)
        conn.close()
        exit(0)

    inputs = []
    while True:
        typed_msg = input()
        if typed_msg.strip() != "":
            inputs.append(typed_msg.strip())
        triggered, params = parse_stdin(inputs)
        if triggered:
            execute_tx(conn, params)
            inputs = []
    conn.close()
