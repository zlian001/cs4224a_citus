import csv
import statistics
import sys
import time


def compute_perf_metrics(total_num_exec_xacts, total_exec_time, latencies):
    total_exec_time = round(total_exec_time, 2)
    throughput = round(total_num_exec_xacts / total_exec_time, 2)
    avg_latency = round((sum(latencies) / len(latencies)) * 1000, 2)
    median_latency = round(statistics.median(latencies) * 1000, 2)
    percentile_95th_latency = round(
        statistics.quantiles(latencies, n=100)[94] * 1000, 2)
    percentile_99th_latency = round(
        statistics.quantiles(latencies, n=100)[98] * 1000, 2)

    print(
        f"Number of executed transactions: {total_num_exec_xacts}", file=sys.stderr)
    print(
        f"Total transaction execution time (in seconds): {total_exec_time}", file=sys.stderr)
    print(
        f"Transaction throughput (number of transactions processed per second): {throughput}", file=sys.stderr)
    print(
        f"Average transaction latency (in ms): {avg_latency}", file=sys.stderr)
    print(
        f"Median transaction latency (in ms): {median_latency}", file=sys.stderr)
    print(
        f"95th percentile transaction latency (in ms): {percentile_95th_latency}", file=sys.stderr)
    print(
        f"99th percentile transaction latency (in ms): {percentile_99th_latency}", file=sys.stderr)

    return (total_num_exec_xacts, total_exec_time, throughput, avg_latency, median_latency, percentile_95th_latency, percentile_99th_latency)


def write_client_csv(results_dir, client_number, metrics_tuple):
    header_fields = ["client_number", "measurement_a", "measurement_b", "measurement_c",
                     "measurement_d", "measurement_e", "measurement_f", "measurement_g"]
    total_num_exec_xacts = metrics_tuple[0]
    total_exec_time = metrics_tuple[1]
    throughput = metrics_tuple[2]
    avg_latency = metrics_tuple[3]
    median_latency = metrics_tuple[4]
    percentile_95th_latency = metrics_tuple[5]
    percentile_99th_latency = metrics_tuple[6]

    with open(f"{results_dir}/clients.csv", "a") as fname:
        writer = csv.writer(fname)

        # write header if file is empty
        if fname.tell() == 0:
            writer.writerow(header_fields)

        print(
            f"client{client_number} writing client metrics to {results_dir}/clients.csv")
        writer.writerow([client_number, total_num_exec_xacts, total_exec_time, throughput,
                        avg_latency, median_latency, percentile_95th_latency, percentile_99th_latency])

    return


def write_throughput_csv(results_dir):
    header_fields = ["min_throughput", "max_throughput", "avg_throughput"]

    txn_throughputs = []
    with open(f"{results_dir}/clients.csv", "r") as clients:
        reader = csv.reader(clients)
        # skip header row
        next(reader)

        for row in reader:
            txn_throughputs.append(float(row[3]))

    min_throughput = min(txn_throughputs)
    max_throughput = max(txn_throughputs)
    avg_throughput = sum(txn_throughputs) / len(txn_throughputs)

    with open(f"{results_dir}/throughput.csv", "w") as fthroughput:
        writer = csv.writer(fthroughput)

        # write header if file is empty
        if fthroughput.tell() == 0:
            writer.writerow(header_fields)

        print(f"writing throughput metrics to {results_dir}/throughput.csv")
        writer.writerow([min_throughput, max_throughput, avg_throughput])

    return


def write_dbstate_csv(session, results_dir):
    queries = [
        'SELECT SUM(W_YTD) FROM warehouse',
        'SELECT SUM(D_YTD), SUM(D_NEXT_O_ID) FROM district',
        'SELECT SUM(C_BALANCE), SUM(C_YTD_PAYMENT), SUM(C_PAYMENT_CNT), SUM(C_DELIVERY_CNT) FROM customer',
        'SELECT MAX(O_ID), SUM(O_OL_CNT) FROM customer_order',
        'SELECT SUM(OL_AMOUNT), SUM(OL_QUANTITY) FROM order_line',
        'SELECT SUM(S_QUANTITY), SUM(S_YTD), SUM(S_ORDER_CNT), SUM(S_REMOTE_CNT) FROM stock'
    ]

    with open(f"{results_dir}/dbstate.csv", "w") as fdbstate:
        print(f"writing dbstate metrics to {results_dir}/dbstate.csv")
        with session:
            with session.cursor() as cur:
                for query in queries:
                    start_time = time.time()
                    rows = session.execute(query)
                    cur.execute(query)
                    rows = cur.fetchall()
                    end_time = time.time()
                    print(
                        f"Query '{query}' took {end_time - start_time} seconds to execute.")
                    for row in rows:
                        for col in row._fields:
                            val = getattr(row, col)
                            fdbstate.write(f"{col}: {val}" + "\n")
