
from . import *
from .params import *
from abc import abstractmethod
import datetime
import time


class Transactions(object):

    def __init__(self, update_batch_size: int, select_batch_size:int):
        self.update_batch_size = update_batch_size
        self.select_batch_size = select_batch_size

    @abstractmethod
    def new_order_transaction(self, m_conn, m_params: NewOrderTxParams) -> float:
        raise NotImplementedError

    def payment_transaction(self, m_conn, m_params: PaymentTxParams) -> float:
        """
        used index: warehouse (w_id)
        used index: district (d_w_id, d_id)
        used index: customer (c_w_id, c_d_id, c_id)

        Payment Transaction consists of one line of input with
        five comma-separated values: P, C_W_ID, C_D_ID, C_ID, PAYMENT.
        eg:
                    P,1,1,2203,3261.87
        """
        c_w_id = m_params.c_w_id
        c_d_id = m_params.c_d_id
        c_id = m_params.c_id
        payment_amount = m_params.payment_amount
        begin = time.time()
        with m_conn.cursor() as cur:
            # cur.execute('SET TRANSACTION PRIORITY HIGH;')

            cur.execute("UPDATE district SET D_YTD = D_YTD + %s "
                        "WHERE D_W_ID = %s and D_ID = %s "
                        "returning D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP",
                        (payment_amount, c_w_id, c_d_id))
            res = cur.fetchone()
            d_street_1 = res[0]
            d_street_2 = res[1]
            d_city = res[2]
            d_state = res[3]
            d_zip = res[4]

            cur.execute("UPDATE warehouse SET W_YTD = W_YTD + %s "
                        "WHERE W_ID = %s RETURNING W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP",
                        (payment_amount, c_w_id))
            res = cur.fetchone()
            w_street_1 = res[0]
            w_street_2 = res[1]
            w_city = res[2]
            w_state = res[3]
            w_zip = res[4]

            cur.execute('''UPDATE customer 
                              SET C_BALANCE = C_BALANCE - %s, 
                                   C_YTD_PAYMENT = C_YTD_PAYMENT + %s, 
                                   C_PAYMENT_CNT = C_PAYMENT_CNT + 1 
                              WHERE C_W_ID = %s and C_D_ID = %s and C_ID = %s 
                              RETURNING C_W_ID, C_D_ID, C_ID, 
                                   C_FIRST, C_MIDDLE, C_LAST, 
                                   C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, 
                                   C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE''',
                        (payment_amount, payment_amount, c_w_id, c_d_id, c_id))

            res = cur.fetchone()
        m_conn.commit()
        end = time.time()
        duration = end - begin

        # print("payment_transaction, "
        #       "w_street_1: %s,"
        #       "w_street_2: %s,"
        #       "w_city: %s,"
        #       "w_state: %s,"
        #       "w_zip: %s," % (w_street_1, w_street_2, w_city, w_state, w_zip))
        #
        # print("payment_transaction, "
        #       "d_street_1: %s,"
        #       "d_street_2: %s,"
        #       "d_city: %s,"
        #       "d_state: %s,"
        #       "d_zip: %s," % (d_street_1, d_street_2, d_city, d_state, d_zip))
        #
        # print("customer:, ", res)
        return duration

    def order_status_transaction(self, m_conn, m_params: OrderStatusTxParams):
        """

        used index: order_ori (o_w_id, o_d_id, ol_o_id) (o_w_id, o_d_id, o_c_id)
        used index: order_line (ol_w_id, ol_d_id, ol_o_id)
        used index: customer (c_w_id, c_d_id, c_id)

        This transaction queries the status of the last order of a customer.
        Order-Status Transaction consists of one line of input with four comma-separated values: O,C W ID,C D ID,C ID.
        eg:
                  O,1,1,1219
                  O,1,2,310
           """

        def print_res(m_res: [tuple]):
            for m_ele in m_res:
                print(
                    "[c_first: %s, c_middle: %s, c_last: %s, c_balance: %s, o_id: %s, o_entry_d: %s, o_carrier_id: %s"
                    "ol_i_id: %s, ol_supply_w_id: %s, ol_quantity: %s, ol_amount: %s, ol_delivery_d: %s]"
                    % (m_ele[0], m_ele[1], m_ele[2],
                       str(m_ele[3]), m_ele[4], str(m_ele[5]),
                       m_ele[6], m_ele[7], m_ele[8], str(m_ele[9]), str(m_ele[10]), str(m_ele[11]))
                )

        w_id = m_params.c_w_id
        d_id = m_params.c_d_id
        c_id = m_params.c_id
        begin = time.time()
        with m_conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-5s'")
            cur.execute('''
                        WITH 
                            customer_ny AS 
                              (select C_ID, C_W_ID, C_D_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE 
                                from customer
                                where (C_ID, C_W_ID, C_D_ID) in ((%s, %s, %s))),
                            order_ny AS 
                              (select o_w_id, o_d_id, o_c_id, o_id, o_entry_d, o_carrier_id 
                                from order_ori 
                                where (o_w_id, o_d_id, o_c_id) in ((%s, %s, %s)) order by o_entry_d desc limit 1)
                            select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, o_id, o_entry_d, o_carrier_id, 
                                   ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d 
                            from order_line 
                            join order_ny ON order_line.ol_w_id=order_ny.o_w_id 
                                and order_line.ol_d_id=order_ny.o_d_id 
                                and order_line.ol_o_id=order_ny.o_id
                            join customer_ny ON order_ny.o_w_id=customer_ny.C_W_ID 
                                and order_ny.o_d_id=customer_ny.C_D_ID 
                                and order_ny.o_c_id=customer_ny.C_ID;
                        ''',
                        (c_id, w_id, d_id, w_id, d_id, c_id))

            res = cur.fetchall()
            # print_res(res)
        m_conn.commit()
        end = time.time()
        duration = end - begin
        return duration

    def stock_level_transaction(self, m_conn, m_params: StockLevelTxParams):
        """

        used index: stock
        used index: order_line (ol_w_id, ol_d_id, ol_o_id)
        used index: district (d_w_id, d_id)

        This transaction examines the items from the last L orders at a specified warehouse district and reports
        the number of those items that have a stock level below a specified threshold.
        """
        w_id = m_params.w_id
        d_id = m_params.d_id
        threshold = m_params.threshold
        l = m_params.l
        begin = time.time()
        with m_conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-5s'")
            # 1. Let N denote value of the next available order number D NEXT O ID for district (W ID,D ID)
            cur.execute("SELECT D_NEXT_O_ID FROM district WHERE D_W_ID = %s AND D_ID = %s", (w_id, d_id))
            res = cur.fetchone()
            n = res[0]

            # 2. Let S denote the set of items from the last L orders for district (W ID,D ID)
            cur.execute(
                '''
                SELECT COUNT(DISTINCT OL_I_ID)
                FROM order_line 
                JOIN stock ON OL_I_ID = stock.S_I_ID AND OL_W_ID = stock.S_W_ID
                WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID >= %s AND OL_O_ID < %s AND S_QUANTITY < %s
                ''', (w_id, d_id, n - l, n, threshold))
            count = cur.fetchone()[0]
        m_conn.commit()
        end = time.time()
        duration = end - begin

        # 3. Output the total number of items in S where its stock quantity at W_ID is below the threshold
        # print("-------------------------------")
        # print(
        #     "the number of items (W_ID: %s, D_ID: %s) with a stock level below the threshold %s within the last %s orders:"
        #     % (w_id, d_id, threshold, l))
        # print(count)

        return duration

    @abstractmethod
    def popular_item_transaction(self, m_conn, m_params: PopItemTxParams) -> float:
        raise NotImplementedError

    def top_balance_transaction(self, m_conn) -> float:
        """
        This transaction finds the top-10 customers ranked in descending order of their outstanding balance
        payments.
        """
        begin = time.time()
        with m_conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-5s'")
            cur.execute(
                '''
                    SELECT c_first, c_middle, c_last, c_balance, c_w_name, c_d_name
                    FROM customer
                    ORDER BY c_balance DESC LIMIT 10;
            ''')
            rows = cur.fetchall()
        m_conn.commit()
        end = time.time()
        duration = end - begin
        # print("-------------------------------")
        # print("Top 10 customers ranked in descending order of outstanding balance:")
        # print("C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, W_NAME, D_NAME")
        # for row in rows:
        #     print(row)
        return duration

    @abstractmethod
    def related_customer_transaction(self, m_conn, m_params: RelCustomerTxParams) -> float:
        raise NotImplementedError

    def delivery_read_transaction(self, m_conn, m_params: DeliveryTxParams):
        """

        used index: order_ori (o_w_id, o_d_id, o_id) (o_w_id, o_d_id, o_carrier_id )
        used index: order_line (ol_w_id, ol_d_id, ol_o_id)
        used index: customer (c_w_id, c_d_id, c_id)

        Order-Status Transaction consists of one line of input with four comma-separated values: O,C W ID,C D ID,C ID.
        eg:
               D,1,6
        """
        w_id = m_params.w_id
        carrier_id = m_params.carrier_id
        begin = time.time()
        with m_conn.cursor() as cur:
            # history read
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-10s'")

            # 1. Let N denote the value of the smallest order number O ID for district (W ID,DISTRICT NO)
            # with O CARRIER ID = null;
            # Let X denote the order corresponding to order number N, and let C denote the customer
            # who placed this order

            # Update the order X by setting O CARRIER ID to CARRIER ID
            query = "select o_d_id, MIN(o_id) from order_ori " \
                    "group by (o_w_id, o_d_id, o_carrier_id) having o_w_id = {} and o_carrier_id is null;".format(w_id)
            cur.execute(query)
            res = cur.fetchall()

            # w_id, d_id, o_id
            id_tuples = [(w_id, ele[0], ele[1]) for ele in res]

            query = "select ol_d_id, sum(ol_amount) from order_line " \
                    "where (ol_w_id, ol_d_id, ol_o_id) in {} group by (ol_w_id, ol_d_id, ol_o_id);". \
                format(id_tuples).replace("[", "(").replace("]", ")")

            cur.execute(query)
            res = cur.fetchall()

            # ol_w_id | ol_d_id | sum
            sum_map = {(w_id, ele[0]): ele[1] for ele in res}

        m_conn.commit()
        end = time.time()
        duration = end - begin
        return duration, w_id, carrier_id, id_tuples, sum_map

    def delivery_update_transaction1(self, m_conn, w_id, carrier_id, id_tuples):
        """
        used index: order_ori (o_w_id, o_d_id, o_id) (o_w_id, o_d_id, o_carrier_id )
        used index: order_line (ol_w_id, ol_d_id, ol_o_id)
        used index: customer (c_w_id, c_d_id, c_id)

        Order-Status Transaction consists of one line of input with four comma-separated values: O,C W ID,C D ID,C ID.
        eg:
               D,1,6
        """
        begin = time.time()
        with m_conn.cursor() as cur:
            # Update the order X by setting O_CARRIER_ID to CARRIER_ID
            query = "update order_ori set o_carrier_id={} where (o_w_id, o_d_id, o_id) in " \
                    "{} returning o_d_id, o_id, o_c_id;".format(carrier_id, id_tuples).replace("[", "("). \
                replace("]", ")")
            cur.execute(query)
            res = cur.fetchall()

            # o_d_id, o_id, o_c_id
            cid_map = {(w_id, ele[0]): ele[2] for ele in res}

            # Update all the order-lines in X by setting OL_DELIVERY D to the current date and time
            query = "update order_line set OL_DELIVERY_D =now() where (ol_w_id, ol_d_id, ol_o_id) in {};". \
                format(id_tuples).replace("[", "(").replace("]", ")")
            cur.execute(query)

        m_conn.commit()
        end = time.time()
        duration = end - begin
        return duration, cid_map

    def delivery_update_transaction2(self, m_conn, c_id, ele, sum_value):
        """
        used index: order_ori (o_w_id, o_d_id, o_id) (o_w_id, o_d_id, o_carrier_id )
        used index: order_line (ol_w_id, ol_d_id, ol_o_id)
        used index: customer (c_w_id, c_d_id, c_id)

        Order-Status Transaction consists of one line of input with four comma-separated values: O,C W ID,C D ID,C ID.
        eg:
               D,1,6
        """
        begin = time.time()
        with m_conn.cursor() as cur:
            # Update customer C as follows:
            # 1. Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the items placed in order X
            # 2. Increment C DELIVERY CNT by 1
            query = "update customer set (C_BALANCE, C_DELIVERY_CNT) = ({},C_DELIVERY_CNT+1) " \
                    "where c_w_id= {} and c_d_id = {} and c_id = {};".format(sum_value, ele[0], ele[1], c_id)
            cur.execute(query)

        m_conn.commit()
        end = time.time()
        duration = end - begin
        return duration

    def delivery_update_transaction3(self, m_conn, w_id, carrier_id, id_tuples, sum_map):

        begin = time.time()
        with m_conn.cursor() as cur:
            # Update all the order-lines in X by setting OL_DELIVERY D to the current date and time
            query = "update order_line set OL_DELIVERY_D =now() where (ol_w_id, ol_d_id, ol_o_id) in {};". \
                format(id_tuples).replace("[", "(").replace("]", ")")
            cur.execute(query)

            # Update the order X by setting O_CARRIER_ID to CARRIER_ID
            query = "update order_ori set o_carrier_id={} where (o_w_id, o_d_id, o_id) in " \
                    "{} returning o_d_id, o_id, o_c_id;".format(carrier_id, id_tuples).replace("[", "("). \
                replace("]", ")")
            cur.execute(query)
            res = cur.fetchall()

            # o_d_id, o_id, o_c_id
            cid_map = {(w_id, ele[0]): ele[2] for ele in res}

            # query = "select * from customer " \
            #         "where (c_w_id, c_d_id ,c_id) in {} FOR UPDATE;".\
            #     format([(ele[0], ele[1], cid_map[ele]) for ele in sum_map.keys()]).\
            #     replace("[", "(").replace("]", ")")
            #
            # cur.execute(query)

            # Update customer C as follows:
            # 1. Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the items placed in order X
            # 2. Increment C DELIVERY CNT by 1
            for ele, sum_value in sum_map.items():
                c_id = cid_map[ele]
                query = "update customer set (C_BALANCE, C_DELIVERY_CNT) = ({},C_DELIVERY_CNT+1) " \
                        "where c_w_id= {} and c_d_id = {} and c_id = {};".format(sum_value, ele[0], ele[1], c_id)
                cur.execute(query)

        m_conn.commit()
        end = time.time()
        duration = end - begin
        return duration

    def delivery_update_transaction4(self, m_conn, cid_map, sum_map):

        begin = time.time()
        with m_conn.cursor() as cur:
            # query = "select * from customer " \
            #         "where (c_w_id, c_d_id ,c_id) in {} FOR UPDATE;".\
            #     format([(ele[0], ele[1], cid_map[ele]) for ele in sum_map.keys()]).\
            #     replace("[", "(").replace("]", ")")
            #
            # cur.execute(query)

            # Update customer C as follows:
            # 1. Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the items placed in order X
            # 2. Increment C DELIVERY CNT by 1
            for ele, sum_value in sum_map.items():
                c_id = cid_map[ele]
                query = "update customer set (C_BALANCE, C_DELIVERY_CNT) = ({},C_DELIVERY_CNT+1) " \
                        "where c_w_id= {} and c_d_id = {} and c_id = {};".format(sum_value, ele[0], ele[1], c_id)
                cur.execute(query)

        m_conn.commit()
        end = time.time()
        duration = end - begin
        return duration

    def delivery_transaction_one_did(self, m_conn, m_params: DeliveryTxParams, d_id):
        """

        used index: order_ori (o_w_id, o_d_id, o_id) (o_w_id, o_d_id, o_carrier_id )
        used index: order_line (ol_w_id, ol_d_id, ol_o_id)
        used index: customer (c_w_id, c_d_id, c_id)

        Order-Status Transaction consists of one line of input with four comma-separated values: O,C W ID,C D ID,C ID.
        eg:
               D,1,6
        """
        w_id = m_params.w_id
        carrier_id = m_params.carrier_id
        begin = time.time()
        with m_conn.cursor() as cur:
            # 1. Let N denote the value of the smallest order number O ID for district (W ID,DISTRICT NO)
            # with O CARRIER ID = null;
            # Let X denote the order corresponding to order number N, and let C denote the customer
            # who placed this order

            # Update the order X by setting O CARRIER ID to CARRIER ID

            query = "update order_ori set o_carrier_id={} where o_w_id={} and o_d_id={} and o_id=" \
                    "(select MIN(o_id) from order_ori where o_w_id={} and o_d_id={} and o_carrier_id is null)  " \
                    "returning o_id, o_c_id;".format(carrier_id, w_id, d_id, w_id, d_id)

            cur.execute(query)

            res = cur.fetchone()
            n = res[0]
            c_id = res[1]

            # Update all the order-lines in X by setting OL DELIVERY D to the current date and time
            query = "update order_line set OL_DELIVERY_D =now() " \
                    "where ol_w_id={} and ol_d_id={} and ol_o_id={};".format(w_id, d_id, n)

            cur.execute(query)

            # Update customer C as follows:
            # 1. Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the items placed in order X
            # 2. Increment C DELIVERY CNT by 1

            query = "update customer set (C_BALANCE, C_DELIVERY_CNT) = " \
                    "((select sum(ol_amount) from order_line " \
                    "   where (ol_w_id, ol_d_id, ol_o_id) in (({}, {}, {})) group by ol_o_id), C_DELIVERY_CNT+1) " \
                    "   where (c_w_id, c_d_id, c_id) in (({}, {}, {}));".format(w_id, d_id, n, w_id, d_id, c_id)
            cur.execute(query)
        m_conn.commit()
        end = time.time()
        duration = end - begin
        return duration


    def delivery_transaction(self, m_conn, m_params: DeliveryTxParams):
        """

        used index: order_ori (o_w_id, o_d_id, o_id) (o_w_id, o_d_id, o_carrier_id )
        used index: order_line (ol_w_id, ol_d_id, ol_o_id)
        used index: customer (c_w_id, c_d_id, c_id)

        Order-Status Transaction consists of one line of input with four comma-separated values: O,C W ID,C D ID,C ID.
        eg:
               D,1,6
        """
        w_id = m_params.w_id
        carrier_id = m_params.carrier_id
        begin = time.time()
        with m_conn.cursor() as cur:
            # 1. Let N denote the value of the smallest order number O ID for district (W ID,DISTRICT NO)
            # with O CARRIER ID = null;
            # Let X denote the order corresponding to order number N, and let C denote the customer
            # who placed this order

            # Update the order X by setting O CARRIER ID to CARRIER ID
            query = "select o_d_id, MIN(o_id) from order_ori " \
                    "group by (o_w_id, o_d_id, o_carrier_id) having o_w_id = {} and o_carrier_id is null;".format(w_id)
            cur.execute(query)
            res = cur.fetchall()

            # w_id, d_id, o_id
            id_tuples = [(w_id, ele[0], ele[1]) for ele in res]

            # Update all the order-lines in X by setting OL_DELIVERY D to the current date and time
            query = "update order_line set OL_DELIVERY_D =now() where (ol_w_id, ol_d_id, ol_o_id) in {};". \
                format(id_tuples).replace("[", "(").replace("]", ")")
            cur.execute(query)

            # Update the order X by setting O_CARRIER_ID to CARRIER_ID
            query = "update order_ori set o_carrier_id={} where (o_w_id, o_d_id, o_id) in " \
                    "{} returning o_d_id, o_id, o_c_id;".format(carrier_id, id_tuples).replace("[", "("). \
                replace("]", ")")
            cur.execute(query)
            res = cur.fetchall()

            # o_d_id, o_id, o_c_id
            cid_map = {(w_id, ele[0]): ele[2] for ele in res}

            query = "select ol_d_id, sum(ol_amount) from order_line " \
                    "where (ol_w_id, ol_d_id, ol_o_id) in {} group by (ol_w_id, ol_d_id, ol_o_id);". \
                format(id_tuples).replace("[", "(").replace("]", ")")

            cur.execute(query)
            res = cur.fetchall()

            # ol_w_id | ol_d_id | sum
            sum_map = {(w_id, ele[0]): ele[1] for ele in res}

            # Update customer C as follows:
            # 1. Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the items placed in order X
            # 2. Increment C DELIVERY CNT by 1
            for ele, sum_value in sum_map.items():
                c_id = cid_map[ele]
                query = "update customer set (C_BALANCE, C_DELIVERY_CNT) = ({},C_DELIVERY_CNT+1) " \
                        "where c_w_id= {} and c_d_id = {} and c_id = {};".format(sum_value, ele[0], ele[1], c_id)
                cur.execute(query)

        m_conn.commit()
        end = time.time()
        duration = end - begin
        return duration

    def test_transaction(self, m_conn):

        k = 110102

        while True:
            with m_conn.cursor() as cur:
                query = "insert INTO cs5424db.workloadA.item (I_ID,I_NAME,I_PRICE,I_IM_ID,I_DATA) " \
                        "values ({}, 'noawtpfynitqxadf' ,330, 108, 'ugmvxubikoestjagbcxiytwclqdosiswxbf')".format(k)

                cur.execute(query)
                # print(cur.fetchone())
                m_conn.commit()
                k += 1
