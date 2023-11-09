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
    database='project',
    user='cs4224a',
    host='xcnd45,xcnd46,xcnd47,xcnd48,xcnd49'
)

write_throughput_csv(RESULTS_DIR)
write_dbstate_csv(conn, RESULTS_DIR)

conn.close()
