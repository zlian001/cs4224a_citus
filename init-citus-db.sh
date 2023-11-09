#!/usr/bin/env bash

# Install Citus database directory  & configure Citus extension

# INSTALLDIR - directory containing installed binaries, libraries, etc.
INSTALLDIR=$HOME/pgsql

TEAM_ID=a # IMPORTANT: change x to your actual team identifier
# DATADIR - directory containing database files
DATADIR=/temp/team${TEAM_ID}-data


mkdir -p ${DATADIR}
${INSTALLDIR}/bin/initdb -D ${DATADIR}
if ! grep -q "citus" ${DATADIR}/postgresql.conf; then
	echo "shared_preload_libraries = 'citus'" >> ${DATADIR}/postgresql.conf
else
	echo "ERROR: ${DATADIR}/postgresql.conf missing!"
fi
