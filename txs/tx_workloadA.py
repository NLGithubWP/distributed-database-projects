from . import *
from .base import Transactions
from .params import *
import time
import datetime


class TxForWorkloadA(Transactions):

    def __init__(self, update_batch_size: int, select_batch_size: int):
        super().__init__(update_batch_size, select_batch_size)

    def new_order_transaction(self, m_conn, m_params: NewOrderTxParams) -> float:
        """
        used index: district (d_w_id, d_id)
        used index: stock (s_w_id, s_i_id )
        used index: item (i_id)
        used index: warehouse (w_id)
        used index: customer (c_w_id, c_d_id, c_id)

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

        # read params
        c_id = m_params.c_id
        w_id = m_params.w_id
        d_id = m_params.d_id
        num_items = m_params.num_items
        item_number = m_params.item_number
        supplier_warehouse = m_params.supplier_warehouse
        quantity = m_params.quantity

        begin = time.time()
        with m_conn.cursor() as cur:

            item_mapper = {item_number[i]: [supplier_warehouse[i], quantity[i]] for i in range(len(item_number))}

            # 1. Let N denote value of the next available order number D NEXT O ID for district (W ID,D ID)
            # 2. Update the district (W ID, D ID) by incrementing D NEXT O ID by one
            cur.execute("UPDATE district SET D_NEXT_O_ID = D_NEXT_O_ID + 1 "
                        "WHERE D_W_ID = %s and D_ID = %s "
                        "returning D_NEXT_O_ID, d_tax;", (w_id, d_id))

            res = cur.fetchone()
            n = int(res[0]) - 1
            d_tax = res[1]

            # 3. Create a new order record, Create a new order
            all_local = 1
            for i in range(num_items):
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
            s_dist_xx = "S_DIST_" + str(d_id)
            s_quantity_list = []

            query = "SELECT S_I_ID, S_QUANTITY FROM stock WHERE (S_W_ID, S_I_ID ) in {} for update".format(
                [(supplier_warehouse[i], item_number[i]) for i in range(len(item_number))]). \
                replace("[", "(").replace("]", ")")
            cur.execute(query)
            res = cur.fetchall()
            quantity_mapper = {ele[0]: ele[1] for ele in res}

            for item_id, values in item_mapper.items():
                supplier_warehouse_value = values[0]
                quantity_value = values[1]
                s_quantity = quantity_mapper[item_id]

                s_quantity_list.append(s_quantity)

                # update stock
                adjusted_qty = s_quantity - quantity_value

                if adjusted_qty < 10:
                    adjusted_qty = adjusted_qty + 100

                if supplier_warehouse_value == w_id:
                    cur.execute(
                        "UPDATE stock SET S_QUANTITY = %s, "
                        "S_YTD = S_YTD + %s,"
                        "S_ORDER_CNT = S_ORDER_CNT + 1 "
                        "WHERE S_W_ID = %s and S_I_ID = %s",
                        (adjusted_qty, quantity_value, supplier_warehouse_value, item_id))
                else:
                    cur.execute(
                        "UPDATE stock SET S_QUANTITY = %s, "
                        "S_YTD = S_YTD + %s,"
                        "S_ORDER_CNT=S_ORDER_CNT+1,"
                        "S_REMOTE_CNT=S_REMOTE_CNT+1 "
                        "WHERE S_W_ID = %s and S_I_ID = %s",
                        (adjusted_qty, quantity_value, supplier_warehouse_value, item_id))

            query = "SELECT I_ID, I_PRICE, I_NAME FROM item WHERE I_ID in {};". \
                format(list(quantity_mapper.keys())).replace("[", "(").replace("]", ")")

            cur.execute(query)
            res = cur.fetchall()
            item_info_mapper = {ele[0]: [ele[1], ele[2]] for ele in res}

            # batch inserting into order_line
            query = "INSERT INTO order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_AMOUNT, " \
                    "OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) values "

            for i in range(len(item_number)):
                item_id = item_number[i]
                values = item_mapper[item_id]
                supplier_warehouse_value = values[0]
                quantity_value = values[1]
                item_price = item_info_mapper[item_id][0]
                item_name = item_info_mapper[item_id][1]

                i_name_list.append(item_name)

                item_amount = quantity_value * item_price
                total_amount += item_amount
                o_amount_list.append(item_amount)

                query += "({},{},{},{},{},{},{},{},'{}')".format(
                    w_id, d_id, n, i, item_id, item_amount, supplier_warehouse_value, quantity_value, s_dist_xx)

                if i < len(item_number) - 1:
                    query += ","

            cur.execute(query)

            # 6. calculate total amount
            cur.execute("SELECT W_TAX FROM warehouse WHERE W_ID = %s", [w_id])
            w_tax = cur.fetchone()[0]

            cur.execute(
                "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM customer WHERE C_W_ID = %s and C_D_ID = %s and C_ID = %s",
                (w_id, d_id, c_id))
            res = cur.fetchone()
            c_discount = res[0]
            c_last = res[1]
            c_credit = res[2]

            total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)
        m_conn.commit()
        end = time.time()
        duration = end - begin

        # print("------------ result is ---------")
        # print("Customer identifier (W ID, D ID, C ID), lastname C_LAST, credit C_CREDIT, discount C_DISCOUNT")
        # print(w_id, d_id, c_id, c_last, c_credit, c_discount)
        # print("Warehouse tax rate W TAX, District tax rate D TAX")
        # print(w_tax, d_tax)
        # print("Order number O_ID, entry date O_ENTRY_D")
        # print(n, entry_d)
        # print("Number of items NUM_ITEMS, Total amount for order TOTAL_AMOUNT")
        # print(num_items, total_amount)
        #
        # for i in range(len(item_number)):
        #     print("ITEM NUMBER[i]: ", item_number[i])
        #     print("I_NAME: ", i_name_list[i])
        #     print("SUPPLIER_WAREHOUSE[i]: ", supplier_warehouse[i])
        #     print("QUANTITY[i]: ", quantity[i])
        #     print("OL_AMOUNT: ", o_amount_list[i])
        #     print("S_QUANTITY: ", s_quantity_list[i])
        return duration

    def popular_item_transaction(self, m_conn, m_params: PopItemTxParams) -> float:
        """
        This transaction finds the most popular item(s) in each of the last L orders at a specified warehouse
        district. Given two items X and Y in the same order O, X is defined to be more popular than Y in O
        if the quantity ordered for X in O is greater than the quantity ordered for Y in O
        """
        w_id = m_params.w_id
        d_id = m_params.d_id
        l = m_params.l
        pop_item_set = set()
        #
        # print("-------------------------------")
        # print("District identifier (W_ID, D_ID): %s, %s" % (w_id, d_id))
        # print("Number of orders to be examined: %s" % (l,))
        begin = time.time()
        with m_conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-5s'")
            # Let N denote value of the next available order number D NEXT O ID for district (W ID,D ID)
            cur.execute("SELECT D_NEXT_O_ID FROM district WHERE D_W_ID = %s AND D_ID = %s", (w_id, d_id))
            res = cur.fetchone()
            n = res[0]
            # print("n is %s" % (n,))

            # Let S denote the set of last L orders for district (W ID,D ID)
            cur.execute(
                '''
                SELECT O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST
                FROM order_ori
                JOIN customer
                ON order_ori.O_W_ID=customer.C_W_ID AND order_ori.O_D_ID=customer.C_D_ID AND order_ori.O_C_ID=customer.C_ID
                WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID >= %s AND O_ID < %s
                ORDER BY O_ID
                ''', (w_id, d_id, n - l, n))
            s = cur.fetchall()

            # Get popular items for each order
            cur.execute(
                '''  WITH ol2 AS
                        (SELECT OL_O_ID, OL_W_ID, OL_D_ID, MAX(OL_QUANTITY) AS MAX
                         FROM order_line
                         WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID >= %s AND OL_O_ID < %s
                         GROUP BY OL_O_ID, OL_W_ID, OL_D_ID)
                    SELECT order_line.OL_O_ID, order_line.OL_I_ID, item.I_NAME, order_line.OL_QUANTITY
                    FROM order_line
                    JOIN item
                    ON order_line.OL_I_ID=item.I_ID
                    INNER JOIN ol2
                    ON order_line.OL_O_ID=ol2.OL_O_ID AND order_line.OL_W_ID=ol2.OL_W_ID AND order_line.OL_D_ID=ol2.OL_D_ID AND OL_QUANTITY=MAX
                    ORDER BY order_line.OL_O_ID
                 ''', (w_id, d_id, n - l, n))
            p_x = cur.fetchall()

            # print results
            j = 0
            for i in range(l):
                # print("order: O_ID, O_ENTRY_ID")
                # print(s[i][0], s[i][1])
                #
                # print("customer name")
                # print(s[i][2], s[i][3], s[i][4])
                #
                # print("popular item: I_NAME, OL_QUANTITY")
                while p_x[j][0] == s[i][0]:
                    # print(p_x[j][2], p_x[j][3])
                    pop_item_set.add(p_x[j][1])
                    j += 1
                    if (j == len(p_x)):
                        break

            # get the number of each item's appearance among order lines, store in a list
            pop_item_set = list(pop_item_set)
            cur.execute("""
                        SELECT OL_I_ID, COUNT(*)
                        FROM order_line
                        WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID >= %s AND OL_O_ID < %s AND OL_I_ID = ANY %s
                        GROUP BY OL_I_ID
                     """, (w_id, d_id, n - l, n, pop_item_set))
            item_count = cur.fetchall()
            # print("items and their counts")
            # print(item_count)
            #
            # # for each distinct popular item, the percentage of orders in S that contain the popular item
            # for item in item_count:
            #     print("%% of orders that contain the popular item with I_ID (%s) is %s %%" % (item[0], (item[1] / l) * 100))

        m_conn.commit()
        end = time.time()
        duration = end - begin
        return duration

    def related_customer_transaction(self, m_conn, m_params: RelCustomerTxParams) -> float:

        w_id = m_params.w_id
        d_id = m_params.d_id
        c_id = m_params.c_id
        related_customers = set()
        begin = time.time()

        # for checking manually
        related_pair = {}

        with m_conn.cursor() as cur:
            # history read
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-5s'")

            cur.execute(
                '''
                SELECT O_ID, O_W_ID, O_D_ID
                FROM order_ori
                WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s
                ''', (w_id, d_id, c_id)
            )
            orders = cur.fetchall()
            wdo_combines = []
            for order in orders:
                o_id = order[0]
                query = "with match_order_item AS " \
                            "(select OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID from order_line " \
                             "where ol_i_id in (SELECT OL_I_ID FROM order_line " \
                                                "WHERE OL_O_ID = {} AND OL_W_ID = {} AND OL_D_ID = {})" \
                             "AND OL_W_ID <> {}) " \
                        "select OL_W_ID, OL_D_ID, OL_O_ID from match_order_item group by OL_W_ID, OL_D_ID, OL_O_ID " \
                        "having count(*) >= 2;".format(o_id, w_id, d_id, w_id)

                cur.execute(query)
                res = cur.fetchall()
                wdo_combines.extend(res)
                if len(res) and (w_id, d_id, c_id, o_id) not in related_pair:
                    # target customer's (w_id, d_id, c_id, o_id): matched customer's (OL_W_ID, OL_D_ID, OL_O_ID)
                    related_pair[(w_id, d_id, c_id, o_id)] = res

            if wdo_combines:
                query = "select distinct O_W_ID, O_D_ID, O_ID, o_c_id from order_ori where (O_W_ID, O_D_ID, O_ID) in {}".\
                    format(set(wdo_combines)).replace("{", "(").replace("}", ")")
                cur.execute(query)
                customers = cur.fetchall()
                related_customers.update(customers)

        m_conn.commit()
        end = time.time()
        duration = end - begin

        # print("-------------------------------")
        # print("check correctness")
        # print("input value, [w_id, d_id, c_id] are")
        # print(w_id, d_id, c_id)
        # print("target customer's (w_id, d_id, c_id, o_id): matched customer's (OL_W_ID, OL_D_ID, OL_O_ID)")
        # print(related_pair)
        # print("And then, check c_id from order_ori table according to matched customer's (OL_W_ID, OL_D_ID, OL_O_ID)")
        # for customer in related_customers:
        #     print(customer)
        #
        # print("Related customers (W_ID, D_ID, C_ID):")
        # for customer in related_customers:
        #     print(customer[0], customer[1], customer[3])
        return duration
