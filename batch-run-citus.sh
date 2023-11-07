#!/bin/bash
#SBATCH --job-name=teamA_citus_batch
#SBATCH --partition=medium
#SBATCH --time=01:00:00
#SBATCH --output=/home/stuproj/cs4224a/cs4224a_citus/slurm_output/citus_batch-%j.out
#SBATCH --error=/home/stuproj/cs4224a/cs4224a_citus/slurm_output/citus_batch-%j.err
#SBATCH --nodelist=xcnc0,xcnc1,xcnc2,xcnc3,xcnc4
#SBATCH --mem-per-cpu=2G   # memory per CPU core
#SBATCH --cpus-per-task=24 # CPUs per srun task

# proj variables
XACTDIR='/temp/cs4224a/project_files/xact_files'
RESULTSDIR='/home/stuproj/cs4224a/cs4224a_cassandra/results'
SCRIPTSDIR="$HOME/project_files/scripts"
CONSISTENCYLEVEL='ONE'

# CITUS node variables
COORD="xcnc0"
WORKERS=("xcnc1,xcnc2,xcnc3,xcnc4")

# define tasks flags with default values
deploy_citus=false
load_data=false
exec_transactions=false

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

# parse script args to switch flags
while getopts dscft flag; do
    case "${flag}" in
        d) deploy_citus=true;;
        l) load_data=true;;
        t) exec_transactions=true;;
    esac
done

# deploy CITUS and project files
if $deploy_citus; then
    srun ${SCRIPTSDIR}/deploy-citus.sh ${COORD} ${WORKERS[@]} &
    #srun --nodes=5 --ntasks=5 --cpus-per-task=16 --nodelist=xcnd40,xcnd41,xcnd42,xcnd43,xcnd44 ${SCRIPTSDIR}/deploy_citus.sh &
    echo $(logtime) "started CITUS on cluster"
    srun cp -rp $HOME/project_files /temp/cs4224a/
    echo $(logtime) "copied project data and xact files to nodes"
fi

# check all 5 nodes are up and in normal state (UN) before executing operations on cluster
#num_un_nodes=0
#while [[ "$num_un_nodes" -ne 5 ]]; do
#    # get CITUS nodes status
#    status=$(nodetool -h 192.168.51.3 status)
#
#    # count the number of nodes that are up and in normal state
#    num_un_nodes=$(echo "$status" | grep -w 'UN' | wc -l)
#
#    # if not all nodes are up and in normal state, sleep for a while before checking again
#    if [[ "$num_un_nodes" -ne 5 ]]; then
#        echo "not all nodes are in UN state, retrying in 10 seconds..."
#        sleep 10
#    fi
#done
#echo $(logtime) "all nodes are in UN state, proceeding with remaining operations..."
#fi

# creating schemas and loading data from COORD node
if $load_data; then
    echo $(logtime) "creating table schemas and loading data using ${COORD}"
    srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd40 ${SCRIPTSDIR}/load_data.sh
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
