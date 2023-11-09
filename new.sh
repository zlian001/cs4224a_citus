srun --nodes=5 --ntasks=5 --cpus-per-task=4 --nodelist=xcnd[20-24] ${INSTALLDIR}/bin/pg_ctl -D ${TEMPDIR} -l ${LOGFILE} -o "-p ${PGPORT}" start
sleep 3600