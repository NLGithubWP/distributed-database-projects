
from . import *
from .base import Transactions
from .params import *


class TxForWorkloadA(Transactions):

    def __init__(self, update_batch_size: int, select_batch_size: int):
        super().__init__(update_batch_size, select_batch_size)

    def new_order_transaction(self, m_conn, m_params: NewOrderTxParams):
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

        # read params
        c_id = m_params.c_id
        w_id = m_params.w_id
        d_id = m_params.d_id
        num_items = m_params.num_items
        item_number = m_params.item_number
        supplier_warehouse = m_params.supplier_warehouse
        quantity = m_params.quantity

        with m_conn.cursor() as cur:

            # 1. Let N denote value of the next available order number D NEXT O ID for district (W ID,D ID)
            # 2. Update the district (W ID, D ID) by incrementing D NEXT O ID by one
            cur.execute("UPDATE district SET D_NEXT_O_ID = D_NEXT_O_ID + 1 "
                        "WHERE D_W_ID = %s and D_ID = %s "
                        "returning D_NEXT_O_ID, d_tax;", (w_id, d_id))

            res = cur.fetchone()
            n = int(res[0])-1
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

            # select all s_quantity
            query = "SELECT S_QUANTITY FROM stock WHERE (S_W_ID,S_I_ID ) in ("
            for i in range(len(item_number)):
                query += "({},{})".format(supplier_warehouse[i], item_number[i])
                if i < len(item_number) - 1:
                    query += ","
            query += ")"
            cur.execute(query)
            s_quantity_list = list(cur.fetchall())

            # select all items
            query = "SELECT I_PRICE, I_NAME FROM item AS OF SYSTEM TIME follower_read_timestamp() " \
                    "WHERE I_ID in {}".format(tuple(item_number))
            cur.execute(query)
            item_price_name = list(cur.fetchall())

            # batch inserting
            query = "INSERT INTO order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_AMOUNT, " \
                    "OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) values "

            for i in range(len(item_number)):
                i_price = item_price_name[i][0]
                item_amount = quantity[i] * i_price
                total_amount += item_amount
                i_name_list.append(item_price_name[i][1])
                o_amount_list.append(item_amount)

                query += "({},{},{},{},{},{},{},{},'{}')".format(
                    w_id, d_id, n, i, item_number[i], item_amount, supplier_warehouse[i], quantity[i], s_dist_xx)

                if i < len(item_number) - 1:
                    query += ","

            cur.execute(query)

            for i in range(0, len(item_number)):
                adjusted_qty = s_quantity_list[i][0]-quantity[i]
                if adjusted_qty < 10:
                    adjusted_qty = adjusted_qty + 100

                if supplier_warehouse[i] == w_id:
                    cur.execute(
                        "UPDATE stock SET S_QUANTITY = %s, "
                        "S_YTD = S_YTD + %s,"
                        "S_ORDER_CNT = S_ORDER_CNT + 1 "
                        "WHERE S_W_ID = %s and S_I_ID = %s",
                        (adjusted_qty, quantity[i], supplier_warehouse[i], item_number[i]))
                else:
                    cur.execute(
                        "UPDATE stock SET S_QUANTITY = %s, "
                        "S_YTD = S_YTD + %s,"
                        "S_ORDER_CNT=S_ORDER_CNT+1,"
                        "S_REMOTE_CNT=S_REMOTE_CNT+1 "
                        "WHERE S_W_ID = %s and S_I_ID = %s",
                        (adjusted_qty, quantity[i], supplier_warehouse[i], item_number[i]))

            # 6.  total amount
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

        print("------------ result is ---------")
        print("Customer identifier (W ID, D ID, C ID), lastname C_LAST, credit C_CREDIT, discount C_DISCOUNT")
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
            print("S_QUANTITY: ", s_quantity_list[i][0])

    def payment_transaction(self, m_conn, m_params: PaymentTxName):
        """
        Payment Transaction consists of one line of input with
        five comma-separated values: P, C_W_ID, C_D_ID, C_ID, PAYMENT.
        eg:
                    P,1,1,2203,3261.87
        """
        c_w_id = m_params.c_w_id
        c_d_id = m_params.c_d_id
        c_id = m_params.c_id
        payment_amount = m_params.payment_amount

        with m_conn.cursor() as cur:
            cur.execute("UPDATE warehouse SET W_YTD = W_YTD + %s "
                        "WHERE W_ID = %s RETURNING W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP",
                        (payment_amount, c_w_id))
            res = cur.fetchone()
            w_street_1 = res[0]
            w_street_2 = res[1]
            w_city = res[2]
            w_state = res[3]
            w_zip = res[4]
            print("payment_transaction, "
                  "w_street_1: %s,"
                  "w_street_2: %s,"
                  "w_city: %s,"
                  "w_state: %s,"
                  "w_zip: %s," % (w_street_1, w_street_2, w_city, w_state, w_zip))

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
            print("payment_transaction, "
                  "d_street_1: %s,"
                  "d_street_2: %s,"
                  "d_city: %s,"
                  "d_state: %s,"
                  "d_zip: %s," % (d_street_1, d_street_2, d_city, d_state, d_zip))

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
            print("customer:, ", res)

            m_conn.commit()

    def delivery_transaction(self, m_conn, m_params: DeliveryTxParams):
        """
        Order-Status Transaction consists of one line of input with four comma-separated values: O,C W ID,C D ID,C ID.
        eg:
               D,1,6
        """
        w_id = m_params.w_id
        carrier_id = m_params.carrier_id
        with m_conn.cursor() as cur:
            for d_id in range(1, 10, 1):
                # 1. Let N denote the value of the smallest order number O ID for district (W ID,DISTRICT NO)
                # with O CARRIER ID = null;
                # Let X denote the order corresponding to order number N, and let C denote the customer
                # who placed this order

                # Update the order X by setting O CARRIER ID to CARRIER ID
                cur.execute('''
                            update order_ori set o_carrier_id = %s 
                            where (o_w_id, o_d_id, o_id) in 
                              (select o_w_id, o_d_id, o_id from order_ori 
                               where o_id = (select MIN(o_id)) from order_ori 
                                where o_w_id = %s and o_d_id = %s and o_carrier_id is null) 
                            returning o_id, o_c_id;''',
                            (carrier_id, w_id, d_id))

                res = cur.fetchone()
                n = res[0]
                c_id = res[1]

                # Update all the order-lines in X by setting OL DELIVERY D to the current date and time
                cur.execute('''
                            update order_line set OL_DELIVERY_D =now() 
                            where (ol_w_id, ol_d_id, ol_o_id) in ((%s, %s, %s))''',
                            (w_id, d_id, n))

                # Update customer C as follows:
                # 1. Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the items placed in order X
                # 2. Increment C DELIVERY CNT by 1

                cur.execute('''
                            update customer set (C_BALANCE, C_DELIVERY_CNT) = 
                               ((select sum(ol_amount) from order_line 
                                 where (ol_w_id, ol_d_id, ol_o_id) in ((%s, %s, %s))
                                 group by ol_o_id ), C_DELIVERY_CNT+1) 
                            where (c_w_id, c_d_id, c_id) in ((%s, %s, %s));''',
                            (w_id, d_id, n, w_id, d_id, c_id))

            m_conn.commit()

    def order_status_transaction(self, m_conn, m_params: OrderStatusTxParams):
        """
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
        with m_conn.cursor() as cur:
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
            print_res(res)
            m_conn.commit()

    def stock_level_transaction(self, m_conn, m_params: StockLevelTxParams):
        """
        This transaction examines the items from the last L orders at a specified warehouse district and reports
        the number of those items that have a stock level below a specified threshold.
        """
        w_id = m_params.w_id
        d_id = m_params.d_id
        threshold = m_params.threshold
        l = m_params.l

        with m_conn.cursor() as cur:
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

            # 3. Output the total number of items in S where its stock quantity at W_ID is below the threshold
            print("-------------------------------")
            print(
                "the number of items (W_ID: %s, D_ID: %s) with a stock level below the threshold %s within the last %s orders:"
                % (w_id, d_id, threshold, l))
            print(count)

    def popular_item_transaction(self, m_conn, m_params: PopItemTxParams):
        """
        This transaction finds the most popular item(s) in each of the last L orders at a specified warehouse
        district. Given two items X and Y in the same order O, X is defined to be more popular than Y in O
        if the quantity ordered for X in O is greater than the quantity ordered for Y in O
        """
        w_id = m_params.w_id
        d_id = m_params.d_id
        l = m_params.l
        pop_item_set = set()

        print("-------------------------------")
        print("District identifier (W_ID, D_ID): %s, %s" % (w_id, d_id))
        print("Number of orders to be examined: %s" % (l,))

        with m_conn.cursor() as cur:
            # Let N denote value of the next available order number D NEXT O ID for district (W ID,D ID)
            cur.execute("SELECT D_NEXT_O_ID FROM district WHERE D_W_ID = %s AND D_ID = %s", (w_id, d_id))
            res = cur.fetchone()
            n = res[0]
            print("n is %s" % (n,))

            # Let S denote the set of last L orders for district (W ID,D ID)
            cur.execute(
                '''
                SELECT O_ID, O_ENTRY_D, O_C_FIRST, O_C_MIDDLE, O_C_LAST
                FROM order_ori 
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
                    SELECT order_line.OL_O_ID, order_line.OL_I_ID, order_line.OL_I_NAME, order_line.OL_QUANTITY
                    FROM order_line
                    INNER JOIN ol2
                    ON order_line.OL_O_ID=ol2.OL_O_ID AND order_line.OL_W_ID=ol2.OL_W_ID AND order_line.OL_D_ID=ol2.OL_D_ID AND OL_QUANTITY=MAX
                    ORDER BY order_line.OL_O_ID
                 ''', (w_id, d_id, n - l, n))
            p_x = cur.fetchall()


            # print results
            j = 0
            for i in range(l):
                print("order: O_ID, O_ENTRY_ID")
                print(s[i][0], s[i][1])

                print("customer name")
                print(s[i][2], s[i][3], s[i][4])

                print("popular item: I_NAME, OL_QUANTITY")
                while p_x[j][0]==s[i][0]:
                    print(p_x[j][2], p_x[j][3])
                    pop_item_set.add(p_x[j][1])
                    j+=1
                    if(j==len(p_x)):
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
            print("items and their counts")
            print(item_count)

            # for each distinct popular item, the percentage of orders in S that contain the popular item
            for item in item_count:
                print("%% of orders that contain the popular item with I_ID (%s) is %s %%" % (item[0], (item[1] / l) * 100))

            m_conn.commit()

    def top_balance_transaction(self, m_conn):
        """
        This transaction finds the top-10 customers ranked in descending order of their outstanding balance
        payments.
        """
        with m_conn.cursor() as cur:
            cur.execute(
                '''
                WITH 
                    top_customers AS
                        (SELECT c_first, c_middle, c_last, c_balance, c_d_id, c_w_id 
                        FROM customer
                        ORDER BY customer.c_balance DESC LIMIT 10)
                    SELECT c_first, c_middle, c_last, c_balance, w_name, d_name
                    FROM top_customers 
                    JOIN district ON d_id = top_customers.c_d_id AND d_w_id = top_customers.c_w_id
                    JOIN warehouse ON w_id = top_customers.c_w_id
                    ORDER BY top_customers.c_balance DESC;
            ''')
            rows = cur.fetchall()
            m_conn.commit()

            print("-------------------------------")
            print("Top 10 customers ranked in descending order of outstanding balance:")
            print("C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, W_NAME, D_NAME")
            for row in rows:
                print(row)

    def related_customer_transaction(self, m_conn, m_params: RelCustomerTxParams):
        """
        This transaction finds all the customers who are related to a specific customer. Given a customer C,
        another customer C is defined to be related to C if C and C are associated with different warehouses,
        and C and C each has placed some order, O and O′respectively,where both O and O′contain at least two items in common.
        """
        w_id = m_params.w_id
        d_id = m_params.d_id
        c_id = m_params.c_id
        related_customers = set()

        with m_conn.cursor() as cur:
            cur.execute(
                '''
                SELECT O_ID, O_W_ID, O_D_ID
                FROM order_ori
                WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s
                ''', (w_id, d_id, c_id)
            )
            orders = cur.fetchall()

            for order in orders:
                cur.execute(
                    '''
                    WITH
                        items AS
                            (SELECT OL_I_ID
                            FROM order_line
                            WHERE OL_O_ID = %s AND OL_W_ID = %s AND OL_D_ID = %s),
                        customer_ol AS
                            (SELECT O_C_ID, O_W_ID, O_D_ID, O_ID, order_line.OL_I_ID
                            FROM order_ori
                            JOIN order_line ON O_W_ID = order_line.OL_W_ID AND O_D_ID = order_line.OL_D_ID AND O_ID = order_line.OL_O_ID
                            WHERE O_W_ID <> %s
                            )
                        SELECT DISTINCT O_W_ID, O_D_ID, O_C_ID
                        FROM items
                        LEFT JOIN customer_ol ON items.OL_I_ID = customer_ol.OL_I_ID
                        GROUP BY O_C_ID, O_W_ID, O_D_ID, O_ID
                        HAVING COUNT(*) >= 2
                    ''', (order[0], w_id, d_id, w_id)
                )
                customers = cur.fetchall()
                related_customers.update(customers)
            m_conn.commit()

            print("-------------------------------")
            print("Related customers (W_ID, D_ID, C_ID):")
            for customer in related_customers:
                print(customer)
