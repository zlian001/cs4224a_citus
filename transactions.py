import psycopg2
from prepared_statements import TXN_QUERIES
from datetime import datetime
from decimal import Decimal



class Transactions:
    def __init__(self, db, user, host):
        self.stmts = TXN_QUERIES
        self.conn = psycopg2.connect(
            database=db,
            user=user,
            host=host
        )

    #payment transaction 2.2
    # def payment_txn(self, c_w_id, c_d_id, c_id, payment):
    #     with self.conn:
    #         with self.conn.cursor() as cur:
    #
    #             cur.execute(self.stmts["PAYMENT_TXN_QUERIES"]["updateWarehouseYtd"], (payment, c_w_id))
    #             cur.execute(self.stmts["PAYMENT_TXN_QUERIES"]["updateDistrictYtd"], (payment, c_w_id, c_d_id))
    #             cur.execute(self.stmts["PAYMENT_TXN_QUERIES"]["updateCustomerPayment"], (payment, payment, c_w_id, c_d_id, c_id))
    #             cur.execute(self.stmts["PAYMENT_TXN_QUERIES"]["getCustomerInfo"], (c_w_id, c_d_id, c_id))
    #             customer_info = cur.fetchone()
    #             print("Customer Info:", customer_info)
    #             print("Payment Amount:", payment)
    #
    #
    #
    #     return

    def payment_txn(self, c_w_id, c_d_id, c_id, payment):
        with self.conn:
            with self.conn.cursor() as cur:
                # Lock customer row
                cur.execute("SELECT * FROM customer WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s FOR UPDATE", (c_w_id, c_d_id, c_id))
                # Update warehouse
                cur.execute(self.stmts["PAYMENT_TXN_QUERIES"]["updateWarehouseYtd"], (payment, c_w_id))
                # Update district
                cur.execute(self.stmts["PAYMENT_TXN_QUERIES"]["updateDistrictYtd"], (payment, c_w_id, c_d_id))
                # Update customer payment
                cur.execute(self.stmts["PAYMENT_TXN_QUERIES"]["updateCustomerPayment"], (payment, payment, c_w_id, c_d_id, c_id))
                # Get customer info
                cur.execute(self.stmts["PAYMENT_TXN_QUERIES"]["getCustomerInfo"], (c_w_id, c_d_id, c_id))
                customer_info = cur.fetchone()
                print("Customer Info:", customer_info)
                print("Payment Amount:", payment)

                self.conn.commit()
        return
    #delivery txn 2.3
    # def delivery_txn(self, W_ID, CARRIER_ID):
    #     with self.conn:
    #         with self.conn.cursor() as cur:
    #             for DISTRICT_NO in range(1, 11):
    #                 cur.execute(self.stmts["DELIVERY_QUERIES"]["getOldestUndeliveredOrder"], (W_ID, DISTRICT_NO))
    #                 result = cur.fetchone()
    #                 N = result[0] if result else None
    #
    #                 if N is not None:
    #                     cur.execute(self.stmts["DELIVERY_QUERIES"]["updateOrderCarrierId"], (CARRIER_ID, N, W_ID, DISTRICT_NO))
    #
    #                     cur.execute(self.stmts["DELIVERY_QUERIES"]["updateOrderLineDeliveryDate"], (datetime.now(), N, W_ID, DISTRICT_NO))
    #
    #                     cur.execute(self.stmts["DELIVERY_QUERIES"]["updateCustomerBalanceAndDeliveryCount"], (N, W_ID, DISTRICT_NO, N, W_ID, DISTRICT_NO))
    #
    #         self.conn.commit()
    #     return
    def delivery_txn(self, W_ID, CARRIER_ID):
        with self.conn:
            with self.conn.cursor() as cur:
                for DISTRICT_NO in range(1, 11):
                    # Get the oldest undelivered order and lock the associated customer row
                    cur.execute("""
                    SELECT o_id, o_c_id FROM customer_order 
                    WHERE o_w_id = %s AND o_d_id = %s AND o_carrier_id IS NULL 
                    ORDER BY o_id ASC FOR UPDATE LIMIT 1
                """, (W_ID, DISTRICT_NO))
                    result = cur.fetchone()
                    N = result[0] if result else None

                    if N is not None:
                        # Update order carrier ID
                        cur.execute(self.stmts["DELIVERY_QUERIES"]["updateOrderCarrierId"], (CARRIER_ID, N, W_ID, DISTRICT_NO))
                        # Update order line delivery date
                        cur.execute(self.stmts["DELIVERY_QUERIES"]["updateOrderLineDeliveryDate"], (datetime.now(), N, W_ID, DISTRICT_NO))
                        # Update customer balance and delivery count
                        cur.execute(self.stmts["DELIVERY_QUERIES"]["updateCustomerBalanceAndDeliveryCount"], (N, W_ID, DISTRICT_NO, N, W_ID, DISTRICT_NO))

                self.conn.commit()
        return


    #top-balance transcations 2.7
    def top_balance_txn(self):
        print("Top Balance Transaction Output:")

        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(self.stmts["TOP_BALANCE_TXN_QUERIES"]["getTopCustomersWithBalances"])
                top_customers = cur.fetchall()

        for customer in top_customers:
            print(f"Customer Name: {customer[0]} {customer[1]} {customer[2]}")
            print(f"Outstanding Balance: {customer[3]}")
            print(f"Warehouse Name: {customer[4]}")
            print(f"District Name: {customer[5]}\n")

        return

    #related-customer transactions 2.8
    def related_customer_txn(self, c_w_id, c_d_id, c_id):
        print(f"Related Customers Transaction Output for warehouse id {c_w_id}, district id {c_d_id}, customer id {c_id}:")

        query = self.stmts["RELATED_CUSTOMER_TXN_QUERIES"]["findRelatedCustomers"]

        with self.conn:
            with self.conn.cursor() as cur:
                # The order of parameters here should match the placeholders in the SQL query.
                cur.execute(query, (c_id, c_d_id, c_w_id, c_w_id))
                related_customers = cur.fetchall()

        for related_customer in related_customers:
            print(f"Customer Identifier: (C W_ID: {related_customer[0]}, C D_ID: {related_customer[1]}, C ID: {related_customer[2]})")

        return

    # define the datatypes for each transaction
    def cast_payment_dtypes(self, params):
        return  [int(params[0]), int(params[1]), int(params[2]), float(params[3])]
    def cast_delivery_dtypes(self, params):
        return [int(params[0]), int(params[1])]

    def cast_top_balance_dtypes(self, params):
        return []

    def cast_related_customer_dtypes(self, params):
        return [int(params[0]), int(params[1]), int(params[2])]


    def close(self):
        self.conn.close()
