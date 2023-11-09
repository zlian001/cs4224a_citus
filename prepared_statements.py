TXN_QUERIES = {
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = %d",  # w_id
        "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = %d AND D_W_ID = %d",  # d_id, w_id
        # d_next_o_id, d_id, w_id
        "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = %d WHERE D_ID = %d AND D_W_ID = %d",
        "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d",  # w_id, d_id, c_id
        # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (%d, %d, %d, %d, %s, %d, %d, %d)",
        # o_id, d_id, w_id
        # "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)",
        "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = %d",  # ol_i_id
        # d_id?, ol_i_id, ol_supply_w_id
        "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = %d AND S_W_ID = %d",
        # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE STOCK SET S_QUANTITY = %d, S_YTD = %d, S_ORDER_CNT = %d, S_REMOTE_CNT = %d WHERE S_I_ID = %d AND S_W_ID = %d",
        # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
        "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (%d, %d, %d, %d, %d, %d, %s, %d, %d, %s)",
    },

    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d",  # w_id, d_id, c_id
        "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d ORDER BY O_ID DESC LIMIT 1;",  # w_id, d_id, c_id
        "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d",  # w_id, d_id, o_id
    },

    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = %d AND D_ID = %d",
        "getStockCount": """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
            WHERE OL_W_ID = %d
              AND OL_D_ID = %d
              AND OL_O_ID < %d
              AND OL_O_ID >= %d
              AND S_W_ID = %d
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < %d;
        """,
    },
}
