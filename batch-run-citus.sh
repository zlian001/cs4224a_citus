#!/bin/bash
#SBATCH --job-name=teamA_citus_batch
#SBATCH --partition=long
#SBATCH --time=03:00:00
#SBATCH --output=/home/stuproj/cs4224a/cs4224a_citus/slurm_output/citus_batch-%j.out
#SBATCH --error=/home/stuproj/cs4224a/cs4224a_citus/slurm_output/citus_batch-%j.err
#SBATCH --nodelist=xcnd[45-49]
#SBATCH --mem-per-cpu=2G   # memory per CPU core
#SBATCH --cpus-per-task=4 # CPUs per srun task

# proj variables
INSTALLDIR=$HOME/pgsql
TEMPDIR='/temp/teama-data'
LOGDIR=${HOME}/cs4224a_citus/logs
LOGFILE=${LOGDIR}/citus-startup-${NODE}.log 2>&1
XACTDIR='/temp/cs4224a/project_files/xact_files'
RESULTSDIR='/home/stuproj/cs4224a/cs4224a_cassandra/results'
SCRIPTSDIR="$HOME/project_files/scripts"

# CITUS node variables
COORD="xcnd45"
WORKERS="xcnd46;xcnd47;xcnd48;xcnd49"
NODE=$(hostname)

# define tasks flags with default values
deploy_citus=false
start_citus=false
load_data=false
exec_transactions=false

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

# parse script args to switch flags
while getopts dscft flag; do
    case "${flag}" in
        d) deploy_citus=true;;
        s) start_citus=true;;
        f) load_data=true;;
        t) exec_transactions=true;;
    esac
done

# deploy CITUS and project files
if $deploy_citus; then
    echo $(logtime) "deploying CITUS cluster"
    srun --nodes=5 --ntasks=5 --cpus-per-task=4 --nodelist=xcnd[45-49] ${SCRIPTSDIR}/deploy-citus.sh ${COORD} ${WORKERS[@]} &
    srun cp -rp $HOME/project_files /temp/cs4224a/
    echo $(logtime) "copied project data and xact files to nodes"
fi

# start CITUS cluster
if $start_citus; then
    echo $(logtime) "starting CITUS cluster"
    srun --nodes=5 --ntasks=5 --cpus-per-task=4 --nodelist=xcnd[45-49] ${INSTALLDIR}/bin/pg_ctl -D ${TEMPDIR} -l ${LOGFILE} -o "-p ${PGPORT}" start
    sleep 60
    echo $(logtime) "node ${NODE}: $(ps -ef | grep postgres | grep -v grep)"
    if [ ${NODE} = "$COORD" ]; then
        echo $(logtime) "node ${NODE}: $( ${INSTALLDIR}/bin/psql -c "SELECT * FROM citus_get_active_worker_nodes();" )"
    fi
    echo $(logtime) "started CITUS cluster"
fi

# creating schemas and loading data from COORD node
if $load_data; then
    echo $(logtime) "creating table schemas and loading data using ${COORD}"
    srun --nodes=5 --ntasks=5 --cpus-per-task=4 --nodelist=xcnd[45-49] ${SCRIPTSDIR}/load-citus-data.sh ${COORD} &
    sleep 3600
fi

# # execute transactions
# if $exec_transactions; then
#     echo $(logtime) "executing transactions"

#     CLIENTSCSVPATH="${RESULTSDIR}/clients.csv"
#     if [[ -f ${CLIENTSCSVPATH} ]]; then
#         echo $(logtime) "found existing ${CLIENTSCSVPATH}. Deleting the file..."
#         rm ${CLIENTSCSVPATH}
#     fi

#     pids=()
#     for i in {0..19}; do
#         server=xcnd$((40 + $i % 5))
#         echo $(logtime) "client${i} executing ${i}.txt from ${server}"
#         srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=${server} python3 ${SCRIPTSDIR}/main_driver.py ${i} ${CONSISTENCYLEVEL} < ${XACTDIR}/${i}.txt 1> ${RESULTSDIR}/client${i}_xacts.log 2> ${RESULTSDIR}/client${i}_xact_metrics.log &
#         pids+=($!)
#     done
    
#     # print out PIDs
#     for pid in ${pids[@]}; do
#         echo $(logtime) "PID: $pid"
#     done
    
#     # wait for all transaction tasks to finish
#     for pid in ${pids[*]}; do
#         wait $pid
#     done

#     # generate performance metrics from xcnd40
#     echo $(logtime) "writing performance metrics to csv files"
#     srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd40 python3 ${SCRIPTSDIR}/generate_metrics_csv.py
# fi

# # gracefully kill cassandra after Xact experiment tasks exits
# # drain the nodes
# echo $(logtime) "draining all cassandra nodes"
# srun --nodes=5 --ntasks=5 --cpus-per-task=2 --nodelist=xcnd40,xcnd41,xcnd42,xcnd43,xcnd44 nodetool drain
# # stop the daemon
# echo $(logtime) "stopping all cassandra node daemons"
# srun --nodes=5 --ntasks=5 --cpus-per-task=2 --nodelist=xcnd40,xcnd41,xcnd42,xcnd43,xcnd44 nodetool stopdaemon
