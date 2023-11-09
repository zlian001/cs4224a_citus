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
        SELECT DISTINCT c2.c_w_id, c2.c_d_id, c2.c_id
        FROM customer_order o
        JOIN order_line ol ON o.o_id = ol.ol_o_id AND o.o_d_id = ol.ol_d_id AND o.o_w_id = ol.ol_w_id
        JOIN (
            SELECT ol2.ol_i_id
            FROM order_line ol2
            JOIN customer_order o2 ON ol2.ol_o_id = o2.o_id AND ol2.ol_d_id = o2.o_d_id AND ol2.ol_w_id = o2.o_w_id
            WHERE o2.o_c_id != %s
        ) as item_common ON ol.ol_i_id = item_common.ol_i_id
        JOIN customer_order co ON item_common.ol_i_id = ol.ol_i_id AND co.o_w_id != %s
        JOIN customer c2 ON co.o_c_id = c2.c_id AND co.o_d_id = c2.c_d_id AND co.o_w_id = c2.c_w_id
        WHERE o.o_c_id = %s AND o.o_d_id = %s AND o.o_w_id = %s
        GROUP BY c2.c_w_id, c2.c_d_id, c2.c_id
        HAVING COUNT(DISTINCT ol.ol_i_id) >= 2;
    """
    }

}
