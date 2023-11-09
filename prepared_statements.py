TXN_QUERIES = {

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
        "updateOrderCarrierId": "UPDATE customer_order SET o_carrier_id = %s WHERE o_id = %s AND o_w_id = %s AND o_d_id = %s",
        "updateOrderLineDeliveryDate": "UPDATE order_line SET ol_delivery_d = %s WHERE ol_o_id = %s AND ol_w_id = %s AND ol_d_id = %s",
        "updateCustomerBalanceAndDeliveryCount": """
    UPDATE customer 
    SET c_balance = COALESCE(c_balance, 0) + 
        (SELECT COALESCE(SUM(ol_amount), 0) FROM order_line 
         WHERE ol_o_id = %s AND ol_w_id = %s AND ol_d_id = %s),
        c_delivery_cnt = c_delivery_cnt + 1 
    WHERE c_id = 
        (SELECT o_c_id FROM customer_order 
         WHERE o_id = %s AND o_w_id = %s AND o_d_id = %s)
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
        WHERE o_c_id = %s AND o_d_id = %s AND o_w_id = %s
    ),

    common_items AS (
        SELECT ol_i_id
        FROM order_line
        WHERE (ol_o_id, ol_d_id, ol_w_id) IN (SELECT o_id, o_d_id, o_w_id FROM selected_customer_orders)
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
    }


}
