#!/usr/bin/env python3

import time
import random
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
import psycopg2


def tx1():
    pass


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


def tx7(conn):
    with conn.cursor() as cur:
        cur.execute(
            '''WITH top_customers AS 
            (SELECT c_first, c_middle, c_last, c_balance, c_d_id, c_w_id FROM customer
            ORDER BY customer.c_balance DESC LIMIT 10)
            SELECT c_first, c_middle, c_last, c_balance, w_name, d_name
            FROM top_customers 
            JOIN district ON d_id=top_customers.c_d_id AND d_w_id=top_customers.c_w_id
            JOIN warehouse ON w_id=top_customers.c_w_id
            ORDER BY top_customers.c_balance DESC;''')
        rows = cur.fetchall()
        conn.commit()
        print("Top 10 customers ranked in descending order of outstanding balance:")
        for row in rows:
            print(row)


def tx8(conn, c_w_id, c_d_id, c_id):
    with conn.cursor() as cur:
        cur.execute("SELECT C_ID FROM customer WHERE C_W_ID != %s", (c_w_id, ))

    conn.commit()


def main():
    opt = parse_cmdline()
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO)

    conn = psycopg2.connect(opt.dsn)
    try:
        if opt.txId == "1":
            tx1()
        elif opt.txId == "2":
            tx2()
        elif opt.txId == "3":
            tx3()
        elif opt.txId == "4":
            tx4()
        elif opt.txId == "5":
            tx5()
        elif opt.txId == "6":
            tx6()
        elif opt.txId == "7":
            tx7(conn)
        elif opt.txId == "8":
            tx8(conn)
        # The function below is used to test the transaction retry logic.  It
        # can be deleted from production code.
        # run_transaction(conn, test_retry_loop)
    except ValueError as ve:
        # Below, we print the error and continue on so this example is easy to
        # run (and run, and run...).  In real code you should handle this error
        # and any others thrown by the database interaction.
        logging.debug("run_transaction(conn, op) failed: %s", ve)
        pass

    conn.close()


def parse_cmdline():
    parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument("dsn", help="""\
            database connection string

            For cockroach demo, use
            'postgresql://<username>:<password>@<hostname>:<port>/cs5424?sslmode=require',
            with the username and password created in the demo cluster, and the hostname
            and port listed in the (sql/tcp) connection parameters of the demo cluster
            welcome message.

            For CockroachCloud Free, use
            'postgres://<username>:<password>@free-tier.gcp-us-central1.cockroachlabs.cloud:26257/<cluster-name>.cs5424?sslmode=verify-full&sslrootcert=<your_certs_directory>/cc-ca.crt'.

            If you are using the connection string copied from the Console, your username,
            password, and cluster name will be pre-populated. Replace
            <your_certs_directory> with the path to the 'cc-ca.crt' downloaded from the
            Console.

            """, default="postgresql://naili:naili@localhost:26257/cs5424?sslmode=require")
    parser.add_argument("-tx", "--txId", help="transaction to be executed, 1 to 8", default="1")
    parser.add_argument("-v", "--verbose", action="store_true", help="print debug info", default="false")
    opt = parser.parse_args()
    print("opt.dsn", opt.dsn)
    print("opt.txId", opt.txId)
    print("opt.verbose", opt.verbose)
    return opt


# python3 cockroachDB_driver.py "postgresql://naili:naili@localhost:26257/cs5424?sslmode=require"
if __name__ == "__main__":
    main()
