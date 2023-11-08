#!/bin/bash

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

INSTALLDIR=$HOME/pgsql
TEMPDIR='/temp/teama-data'
SCRIPTSDIR="$HOME/project_files/scripts"
NODE=$(hostname)
COORD=$1
WORKERS=$2
IFS=";" read -ra my_array <<< "$WORKERS"
LOGDIR=${HOME}/cs4224a_citus/logs
LOGFILE=${LOGDIR}/citus-startup-${NODE}.log 2>&1

echo $(logtime) "node ${NODE}: starting CITUS deployment"
# install CITUS if not exists
if [ ! -d ${TEMPDIR} ]; then
    echo $(logtime) "node ${NODE}: installing CITUS from scripts"
    source ${SCRIPTSDIR}/install-citus.sh
    source ${SCRIPTSDIR}/init-citus-db.sh
    createdb $PGDATABASE
    echo "listen_addresses = '*'" >> ${TEMPDIR}/postgresql.conf
    if [ ! ${NODE} = "$COORD" ]; then
        echo "host    all             all             $(nslookup $COORD | awk '/^Address: / { print $2 }')/32              trust" >> ${TEMPDIR}/pg_hba.conf
        for i in "${my_array[@]}"; do
            if [ ${NODE} != $i ]; then
                echo "host    all             all             $(nslookup $i | awk '/^Address: / { print $2 }')/32              trust" >> ${TEMPDIR}/pg_hba.conf
            fi
        done
    else
        for i in "${my_array[@]}"; do
            echo "host    all             all             $(nslookup $i | awk '/^Address: / { print $2 }')/32              trust" >> ${TEMPDIR}/pg_hba.conf
        done
    fi
fi

# start Citus cluster
echo $(logtime) "node ${NODE}: starting CITUS Server"
if [[ ! -d "${LOGDIR}" ]]; then
    echo $(logtime) "node ${NODE}: creating ${LOGDIR}"
    mkdir -p "${LOGDIR}"
fi

if [ ! ${NODE} = "$COORD" ]; then
    echo "host    all             all             $(nslookup $COORD | awk '/^Address: / { print $2 }')/32              trust" >> ${TEMPDIR}/pg_hba.conf
    for i in "${my_array[@]}"; do
        if [ ${NODE} != $i ]; then
            echo "host    all             all             $(nslookup $i | awk '/^Address: / { print $2 }')/32              trust" >> ${TEMPDIR}/pg_hba.conf
        fi
    done
else
    for i in "${my_array[@]}"; do
        echo "host    all             all             $(nslookup $i | awk '/^Address: / { print $2 }')/32              trust" >> ${TEMPDIR}/pg_hba.conf
    done
fi

#/home/stuproj/cs4224a/pgsql/bin/pg_ctl -D /temp/teama-data -l logfile start
${INSTALLDIR}/bin/pg_ctl -D ${TEMPDIR} -l ${LOGFILE} -o "-p ${PGPORT}" start
${INSTALLDIR}/bin/psql -c "CREATE EXTENSION citus;"
echo $(logtime) "node ${NODE}: $(ps -ef | grep postgres | grep -v grep)"
# coordinator node only
sleep 60
if [ ${NODE} = "$COORD" ]; then
    # register the hostname that future workers will use to connect to the coordinator node
    ${INSTALLDIR}/bin/psql -c "SELECT citus_set_coordinator_host('${COORD}', $PGPORT);"
    echo $(logtime) "node ${NODE}: COORD Node registered."
    for i in "${my_array[@]}"; do
        ${INSTALLDIR}/bin/psql -c "SELECT * from citus_add_node('$i', $PGPORT);"
        echo $(logtime) "node ${NODE}: WORKER Node $i added."
    done
    echo $(logtime) "node ${NODE}: $( ${INSTALLDIR}/bin/psql -c "SELECT * FROM citus_get_active_worker_nodes();" )"
fi

echo $(logtime) "node ${NODE}: completed CITUS deployment"
