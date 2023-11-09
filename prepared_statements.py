TXN_QUERIES = {
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = %s",  # w_id
        "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = %s AND D_W_ID = %s",  # d_id, w_id
        # d_next_o_id, d_id, w_id
        "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = %s WHERE D_ID = %s AND D_W_ID = %s",
        "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s",  # w_id, d_id, c_id
        # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createOrder": "INSERT INTO CUSTOMER_ORDER (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        # o_id, d_id, w_id
        # "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)",
        "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = %s",  # ol_i_id
        # d_id?, ol_i_id, ol_supply_w_id
        "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = %s AND S_W_ID = %s",
        # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE STOCK SET S_QUANTITY = %s, S_YTD = %s, S_ORDER_CNT = %s, S_REMOTE_CNT = %s WHERE S_I_ID = %s AND S_W_ID = %s",
        # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
        "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
    },

    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s",  # w_id, d_id, c_id
        "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM CUSTOMER_ORDER WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s ORDER BY O_ID DESC LIMIT 1;",  # w_id, d_id, c_id
        "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s",  # w_id, d_id, o_id
    },

    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = %s AND D_ID = %s",
        "getStockCount": """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
            WHERE OL_W_ID = %s
              AND OL_D_ID = %s
              AND OL_O_ID < %s
              AND OL_O_ID >= %s
              AND S_W_ID = %s
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < %s;
        """,
    },

    "PAYMENT_TXN_QUERIES":{
        "updateWarehouseYtd": "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?",
        "updateDistrictYtd": "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?",
        "updateCustomerPayment": """
        UPDATE customer
        SET c_balance = c_balance - ?,
            c_ytd_payment = c_ytd_payment + ?,
            c_payment_cnt = c_payment_cnt + 1
        WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;
    """,
        "getCustomerInfo": """
        SELECT c_w_id, c_d_id, c_id, c_first, c_middle, c_last,
               c_street_1, c_street_2, c_city, c_state, c_zip,
               c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance,
               w_street_1, w_street_2, w_city, w_state, w_zip,
               d_street_1, d_street_2, d_city, d_state, d_zip
        FROM customer
        JOIN warehouse ON c_w_id = w_id
        JOIN district ON c_w_id = d_w_id AND c_d_id = d_id
        WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;
    """
    },
    "DELIVERY_QUERIES" : {
        "getOldestUndeliveredOrder": "SELECT MIN(o_id) FROM customer_order WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id IS NULL",
        "updateOrderCarrierId": "UPDATE customer_order SET o_carrier_id = ? WHERE o_id = ? AND o_w_id = ? AND o_d_id = ?",
        "updateOrderLineDeliveryDate": "UPDATE order_line SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_w_id = ? AND ol_d_id = ?",
        "updateCustomerBalanceAndDeliveryCount": """
        UPDATE customer 
        SET c_balance = COALESCE(c_balance, 0) + 
            (SELECT COALESCE(SUM(ol_amount), 0) FROM order_line 
             WHERE ol_o_id = ? AND ol_w_id = ? AND ol_d_id = ?),
            c_delivery_cnt = c_delivery_cnt + 1 
        WHERE c_id = 
            (SELECT o_c_id FROM customer_order 
             WHERE o_id = ? AND o_w_id = ? AND o_d_id = ?)
    """
    },
    "TOP_BALANCE_TXN_QUERIES": {
        "getTopCustomersWithBalances": """
            SELECT c.c_first, c.c_middle, c.c_last, c.c_balance, w.w_name, d.d_name
            FROM customer c
            JOIN warehouse w ON c.c_w_id = w.w_id
            JOIN district d ON c.c_d_id = d.d_id AND c.c_w_id = d.d_w_id
            ORDER BY c.c_balance DESC
            LIMIT 10
        """
    },
    "RELATED_CUSTOMER_TXN_QUERIES": {
        "findRelatedCustomers": """
        SELECT DISTINCT c2.c_w_id, c2.c_d_id, c2.c_id
        FROM customer_order o
        JOIN order_line ol ON o.o_id = ol.ol_o_id AND o.o_d_id = ol.ol_d_id AND o.o_w_id = ol.ol_w_id
        JOIN (
            SELECT ol2.ol_i_id
            FROM order_line ol2
            JOIN customer_order o2 ON ol2.ol_o_id = o2.o_id AND ol2.ol_d_id = o2.o_d_id AND ol2.ol_w_id = o2.o_w_id
            WHERE o2.o_c_id != ?
        ) as item_common ON ol.ol_i_id = item_common.ol_i_id
        JOIN customer_order co ON item_common.ol_i_id = ol.ol_i_id AND co.o_w_id != ?
        JOIN customer c2 ON co.o_c_id = c2.c_id AND co.o_d_id = c2.c_d_id AND co.o_w_id = c2.c_w_id
        WHERE o.o_c_id = ? AND o.o_d_id = ? AND o.o_w_id = ?
        GROUP BY c2.c_w_id, c2.c_d_id, c2.c_id
        HAVING COUNT(DISTINCT ol.ol_i_id) >= 2;
    """
    },

}
