# Wholesale Supplier (CITUS)

This readme describes the steps to configure, setup and run the wholesales supplier application with slurm.

## Deployment and Setup

CITUS installation and setup are automated through deploy-citus.sh

### 1. Citus Table Distribution

CITUS tables can be sharded by a chosen tenant (warehouse) and table shards of the same tenant are co-located together.

```
# below citus-specific cmds are dynamically run by deploy-citus.sh
psql -c "CREATE EXTENSION citus;"
psql -c "SELECT citus_set_coordinator_host('SOME-HOSTNAME', SOME-PORT);"
psql -c "SELECT * from citus_add_node('SOME-HOSTNAME', SOME-PORT);"
```

## Running the benchmarking experiments

`/home/stuproj/cs4224a/project_files/scripts/batch_run_citus.sh` script does the installation of CITUS, runs the cluster, initialises the data and also runs the main driver to execute Xacts on the cluster.

Below is a description of the options accepted by `batch_run_citus.sh`

```
# batch_run_citus.sh options
-d: deploy, setup and start Citus DB on the compute nodes
-l: create schemas, distribute table shards, and load data from project_files/data folder into Citus DB
-t: execute Xacts on Citus cluster with 20 clients and the given *.txt files.
```
