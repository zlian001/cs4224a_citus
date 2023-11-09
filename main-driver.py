import json
import sys
import time

from metrics import *
from transactions import *

# get cmdline args
client_number = int(sys.argv[1])
CLUSTER_IPS = json.loads(sys.argv[2])
print(f"python main driver cluster node IPs: {CLUSTER_IPS}")

# create an instance of the Transaction class
xact = Transactions("project", "cs4224a", CLUSTER_IPS)

# mapping for transaction functions
txn_funcs_dict = {
    "N": (xact.new_order_txn, xact.cast_new_order_dtypes),
    "P": (xact.payment_txn, xact.cast_payment_dtypes),
    "D": (xact.delivery_txn, xact.cast_delivery_dtypes),
    "O": (xact.order_status_txn, xact.cast_order_status_dtypes),
    "S": (xact.stock_level_txn, xact.cast_stock_level_dtypes),
    "I": (xact.popular_item_txn, xact.cast_popular_item_dtypes),
    "T": (xact.top_balance_txn, xact.cast_top_balance_dtypes),
    "R": (xact.related_customer_txn, xact.cast_related_customer_dtypes),
}

total_num_exec_xacts = 0
total_exec_time = 0
latencies = []

# read transactions from stdin
for line in sys.stdin:
    # split csv line params
    params = line.strip().split(",")

    # get correct transaction function and parameter data type conversion function from dict
    txn_type = params[0]
    # TODO: remove this test txn_type condition for PROD
    if txn_type not in ["N", "O", "S"]:
        continue
    txn_func, txn_dtypes_func = txn_funcs_dict[txn_type]

    # convert parameters to correct data types
    converted_params = txn_dtypes_func(params[1:])

    # handle new order Xact N
    if txn_type == "N":
        item_number, supplier_warehouse, quantity = [], [], []
        num_items = converted_params[-1]

        for _ in range(num_items):
            item = next(sys.stdin)
            item_params = item.strip().split(",")
            item_number.append(int(item_params[0]))
            supplier_warehouse.append(int(item_params[1]))
            quantity.append(int(item_params[2]))

        converted_params.extend([item_number, supplier_warehouse, quantity])

    # execute transaction function with converted params
    txn_start_time = time.time()
    if (converted_params):
        txn_func(*converted_params)
    else:
        txn_func()
    txn_end_time = time.time()

    # record statistics
    latency = txn_end_time - txn_start_time
    latencies.append(latency)
    total_num_exec_xacts += 1
    total_exec_time += latency

# compute metrics and write to require csv files
metrics_tuple = compute_perf_metrics(
    total_num_exec_xacts, total_exec_time, latencies)
write_client_csv(RESULTS_DIR, client_number, metrics_tuple)

# close CITUS cluster connections
xact.close()
