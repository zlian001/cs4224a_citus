TXN_QUERIES = {
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = %s",  # w_id
        "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = %s AND D_ID = %s",  # w_id, d_id
        # d_next_o_id, w_id, d_id
        "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = %s WHERE D_W_ID = %s AND D_ID = %s",
        "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s",  # w_id, d_id, c_id
        # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createOrder": "INSERT INTO CUSTOMER_ORDER (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        # o_id, d_id, w_id
        # "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)",
        "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = %s",  # ol_i_id
        # d_id?, ol_supply_w_id, ol_i_id
        "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_W_ID = %s AND S_I_ID = %s",
        # s_quantity, s_order_cnt, s_remote_cnt, ol_supply_w_id, ol_i_id
        "updateStock": "UPDATE STOCK SET S_QUANTITY = %s, S_YTD = %s, S_ORDER_CNT = %s, S_REMOTE_CNT = %s WHERE S_W_ID = %s AND S_I_ID = %s",
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

    "PAYMENT_TXN_QUERIES": {
        "updateWarehouseYtd": "UPDATE warehouse SET w_ytd = w_ytd + %s WHERE w_id = %s",
        "updateDistrictYtd": "UPDATE district SET d_ytd = d_ytd + %s WHERE d_w_id = %s AND d_id = %s",
        "updateCustomerPayment": """
            UPDATE customer
            SET c_balance = c_balance - %s,
                c_ytd_payment = c_ytd_payment + %s,
                c_payment_cnt = c_payment_cnt + 1
            WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s;
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
            WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s;
        """
    },

    "DELIVERY_QUERIES": {
        "getOldestUndeliveredOrder": "SELECT MIN(o_id) FROM customer_order WHERE o_w_id = %s AND o_d_id = %s AND o_carrier_id IS NULL",
        "updateOrderCarrierId": "UPDATE customer_order SET o_carrier_id = %s WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s",
        "updateOrderLineDeliveryDate": "UPDATE order_line SET ol_delivery_d = %s WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s",
        "updateCustomerBalanceAndDeliveryCount": """
            UPDATE customer 
            SET c_balance = COALESCE(c_balance, 0) + 
                (SELECT COALESCE(SUM(ol_amount), 0) FROM order_line 
                WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s),
                c_delivery_cnt = c_delivery_cnt + 1 
            WHERE c_id = 
                (SELECT o_c_id FROM customer_order 
                WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s)
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
            WITH selected_customer_orders AS (
                SELECT o_id, o_d_id, o_w_id
                FROM customer_order
                WHERE o_w_id = %s AND o_d_id = %s AND o_c_id = %s
            ),

            common_items AS (
                SELECT ol_i_id
                FROM order_line
                WHERE (ol_w_id, ol_d_id, ol_o_id) IN (SELECT o_w_id, o_d_id, o_id FROM selected_customer_orders)
                GROUP BY ol_i_id
                HAVING COUNT(ol_i_id) > 1
            ),

            related_orders AS (
                SELECT o.o_c_id, o.o_w_id, o.o_d_id
                FROM order_line ol
                INNER JOIN common_items ci ON ol.ol_i_id = ci.ol_i_id
                INNER JOIN customer_order o ON ol.ol_o_id = o.o_id AND ol.ol_d_id = o.o_d_id AND ol.ol_w_id = o.o_w_id
                WHERE o.o_w_id != %s
                GROUP BY o.o_c_id, o.o_w_id, o.o_d_id
            ),

            related_customers AS (
                SELECT c.c_id, c.c_w_id, c.c_d_id
                FROM customer c
                INNER JOIN related_orders ro ON c.c_id = ro.o_c_id AND c.c_w_id = ro.o_w_id AND c.c_d_id = ro.o_d_id
            )
            
            SELECT DISTINCT rc.c_w_id, rc.c_d_id, rc.c_id
            FROM related_customers rc;
        """
    },

    "POPULAR_ITEM_TXN_QUERIES": {
        "findPopularItems": """
            WITH district_next_order AS (
                SELECT D_NEXT_O_ID
                FROM district
                WHERE D_W_ID = %s AND D_ID = %s
            ),

            last_l_orders AS (
                SELECT O_ID, O_ENTRY_D, O_C_ID, O_W_ID, O_D_ID
                FROM customer_order
                WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID < (SELECT D_NEXT_O_ID FROM district_next_order) AND O_ID >= (SELECT D_NEXT_O_ID - %s FROM district_next_order)
                ORDER BY O_ID DESC
                LIMIT %s
            ),

            popular_items AS (
                SELECT OL.OL_O_ID, OL.OL_I_ID, I.I_NAME, OL.OL_QUANTITY
                FROM order_line OL
                JOIN item I ON OL.OL_I_ID = I.I_ID
                WHERE (OL.OL_W_ID, OL.OL_D_ID, OL_O_ID) IN (SELECT O_W_ID, O_D_ID, O_ID FROM last_l_orders)
            ),

            max_quantity_items AS (
                SELECT OL_O_ID, OL_I_ID, I_NAME, OL_QUANTITY
                FROM popular_items
                WHERE (OL_O_ID, OL_QUANTITY) IN (
                    SELECT OL_O_ID, MAX(OL_QUANTITY)
                    FROM popular_items
                    GROUP BY OL_O_ID
                )
            ),

            item_popularity AS (
                SELECT I_NAME, COUNT(DISTINCT OL_O_ID)::FLOAT / %s * 100 AS popularity_percentage
                FROM max_quantity_items
                GROUP BY I_NAME
            )

            SELECT
                LLO.O_ID,
                LLO.O_ENTRY_D,
                C.C_FIRST,
                C.C_MIDDLE,
                C.C_LAST,
                MQI.I_NAME,
                MQI.OL_QUANTITY,
                IP.popularity_percentage
            FROM
                last_l_orders LLO
            JOIN customer C ON LLO.O_C_ID = C.C_ID
            JOIN max_quantity_items MQI ON LLO.O_ID = MQI.OL_O_ID
            JOIN item_popularity IP ON MQI.I_NAME = IP.I_NAME
        """
    }
}
