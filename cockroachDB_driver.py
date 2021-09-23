#!/usr/bin/env python3

import time
import random
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
import psycopg2
from psycopg2.errors import SerializationFailure


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


def tx7():
    pass


def tx8():
    pass


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
            tx7()
        elif opt.txId == "8":
            tx8()
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
