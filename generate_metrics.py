import json
import sys
import psycopg2

from metrics import write_dbstate_csv, write_throughput_csv

# get cmdline args
CLUSTER_IPS = sys.argv[1]
RESULTS_DIR = sys.argv[2]
print(f"generate_metrics.py cluster node IPs: {CLUSTER_IPS}")

# Define an execution profile for consistency level
conn = psycopg2.connect(
    database=db,
    user=user,
    host=host
)

with conn:
    with conn.cursor() as cur:
        cur.execute(self.stmts["getWarehouseTaxRate"], (w_id,))
        W = cur.fetchone()
        cur.execute(self.stmts["getDistrict"], (d_id, w_id))
        D = cur.fetchone()
        cur.execute(self.stmts["incrementNextOrderId"],
                    (D.d_next_o_id + 1, d_id, w_id))
        cur.execute(self.stmts["getCustomer"], (w_id, d_id, c_id))
        C = cur.fetchone()

write_throughput_csv(RESULTS_DIR)
write_dbstate_csv(conn, RESULTS_DIR)

conn.close()
