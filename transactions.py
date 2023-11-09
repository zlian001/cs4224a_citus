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

    def new_order_txn(self, c_id, w_id, d_id, num_items, item_number, supplier_warehouse, quantity):
        # get table values
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(self.stmts["getWarehouseTaxRate"], (w_id,))
                W = cur.fetchone()
                cur.execute(self.stmts["getDistrict"], (d_id, w_id))
                D = cur.fetchone()
                cur.execute(self.stmts["incrementNextOrderId"],
                            (D.d_next_o_id + 1, d_id, w_id))
                cur.execute(self.stmts["getCustomer"], (w_id, d_id, c_id))
                C = cur.fetchone()

        # create new order
        if any(sw_id != w_id for sw_id in supplier_warehouse):
            o_all_local = 0
        else:
            o_all_local = 1
        curr_dt = datetime.utcnow()
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(self.stmts["createOrder"], (D.d_next_o_id, d_id, w_id, c_id, curr_dt,
                                                        None, num_items, o_all_local))

        # init total amt
        ttl_amt = Decimal(0)
        items = []
        for i in range(num_items):
            # get current item number, supplier warehouse id and item quantity
            curr_s_i_id = item_number[i]
            curr_s_wh_id = supplier_warehouse[i]
            curr_deci_qty = Decimal(quantity[i])

            # update stock
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute(self.stmts["getItemInfo"], (curr_s_i_id,))
                    I = cur.fetchone()
                    cur.execute(self.stmts["getStockInfo"] % (d_id,),
                                (curr_s_i_id, curr_s_wh_id))
                    S = cur.fetchone()
                    adj_qty = S.s_quantity - curr_deci_qty

                    if adj_qty < 10:
                        adj_qty = adj_qty + 100

                    if supplier_warehouse == w_id:
                        cur.execute(self.stmts["updateStock"], (adj_qty, S.s_ytd + curr_deci_qty, S.s_order_cnt + 1,
                                                                w_id, curr_s_i_id))
                    else:
                        cur.execute(self.stmts["updateStock"], (adj_qty, S.s_ytd + curr_deci_qty, S.s_order_cnt + 1,
                                                                S.s_remote_cnt + 1, w_id, curr_s_i_id))

            # get item table tables and compute required amounts
            item_amt = curr_deci_qty * I.i_price
            ttl_amt += item_amt

            # create new order-line
            ol_dist_info = f"S_DIST_{d_id:02d}"
            with self.conn:
                with self.conn.cursor() as cur:
                    # for loop 0-based indexing, manually do i + 1 for OL_NUMBER 1-based indexing
                    cur.execute(self.stmts["createOrderLine"], (D.d_next_o_id, d_id, w_id, i + 1, curr_s_i_id, curr_s_wh_id,
                                                                None, curr_deci_qty, item_amt, ol_dist_info))

            # get curent order item data
            item = {
                'ITEM_NUMBER': curr_s_i_id,
                'I_NAME': I.i_name,
                'SUPPLIER_WAREHOUSE': curr_s_wh_id,
                'QUANTITY': curr_deci_qty,
                'OL_AMOUNT': item_amt,
                'S_QUANTITY': adj_qty
            }
            # add order item to list of items for output
            items.append(item)

        # modify ttl_amt
        ttl_amt = ttl_amt * (1 + W.w_tax + D.d_tax) * (1 - C.c_discount)

        print("New Order Transaction Output:")
        print(
            f"Customer's identifier: ({C.c_w_id}, {C.c_d_id}, {C.c_id}), lastname: {C.c_last}, credit: {C.c_credit}, discount: {C.c_discount}")
        print(f"Warehouse tax rate: {W.w_tax}, District tax rate: {D.d_tax}")
        print(f"Order number: {D.d_next_o_id}, entry date: {curr_dt}")
        print(
            f"Number of items: {num_items}, total amount for order: {ttl_amt}")
        for item in items:
            print(f"Item Number: {item['ITEM_NUMBER']}")
            print(f"I_Name: {item['I_NAME']}")
            print(f"Supplier_Warehouse: {item['SUPPLIER_WAREHOUSE']}")
            print(f"Quantity: {item['QUANTITY']}")
            print(f"OL_Amount: {item['OL_AMOUNT']}")
            print(f"S_Quantity: {item['S_QUANTITY']}")

        return

    def order_status_txn(self, c_w_id, c_d_id, c_id):
        # get customer's last order
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(self.stmts["getCustomerByCustomerId"],
                            (c_w_id, c_d_id, c_id))
                C = cur.fetchone()
                cur.execute(self.stmts["getLastOrder"],
                            (c_w_id, c_d_id, c_id))
                O = cur.fetchone()
                cur.execute(self.stmts["getOrderLines"],
                            (c_w_id, c_d_id, O.o_id))
                OL = cur.fetchall()

        print(f"Order-Status Transaction Output:")
        print(f"Customer's name: ({C.c_first}, {C.c_middle}, {C.c_last})")
        print(f"Customer's balance: {C.c_balance}")
        print(f"Customers last order:")
        print(f"Order number: {O.o_id}")
        print(f"Entry date and time: {O.o_entry_d}")
        print(f"Carrier identifier: {O.o_carrier_id}")

        # fetch each item in customer's last order
        if OL is not None:
            for i, row in enumerate(OL, start=1):
                print(f"item_{i}:")
                for col in row._fields:
                    val = getattr(row, col)
                    print(f"{col}: {val}")

        return

    def stock_level_txn(self, w_id, d_id, T, L):
        print(
            f"Stock-Level Transaction Output for warehouse id {w_id}, district id {d_id}, stock threshold {T} and number of last orders to be examined {L}:")

        # get next available order number
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(self.stmts["getOId"], (w_id, d_id))
                N = cur.fetchone()

        # get range of order number to query
        order_no_range = N - L

        # get list of item ids that falls within range of N-L
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(self.stmts["getStockCount"],
                            (w_id, d_id, N, order_no_range, w_id, T))
                no = cur.fetchone()

        print(f"No. of item numbers found: {no}")

        return

    # define the datatypes for each transaction
    def cast_new_order_dtypes(self, params):
        return [int(params[0]), int(params[1]), int(params[2]), int(params[3])]

    def cast_order_status_dtypes(self, params):
        return [int(params[0]), int(params[1]), int(params[2])]

    def cast_stock_level_dtypes(self, params):
        return [int(params[0]), int(params[1]), int(params[2]), int(params[3])]

    def close(self):
        self.conn.close()
