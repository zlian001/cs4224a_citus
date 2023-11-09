#!/bin/bash
#SBATCH --job-name=teamA_citus_batch
#SBATCH --partition=long
#SBATCH --time=03:00:00
#SBATCH --output=/home/stuproj/cs4224a/cs4224a_citus/slurm_output/citus_batch-%j.out
#SBATCH --error=/home/stuproj/cs4224a/cs4224a_citus/slurm_output/citus_batch-%j.err
#SBATCH --nodelist=xcnd[20-24]
#SBATCH --mem-per-cpu=2G   # memory per CPU core
#SBATCH --cpus-per-task=4 # CPUs per srun task

srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd20 ${INSTALLDIR}/bin/pg_ctl -D ${TEMPDIR} -l ${LOGFILE} -o "-p ${PGPORT}" start
sleep 3600