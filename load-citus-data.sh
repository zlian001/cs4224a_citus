#!/bin/bash
DATADIR='/temp/cs4224a/project_files/data_files'
INSTALLDIR=$HOME/pgsql
SCRIPTSDIR="$HOME/project_files/scripts"

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}
NODE=$(hostname)

${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -f ${SCRIPTSDIR}/schema.sql
echo $(logtime) "created table schemas"
${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c
\copy WAREHOUSE from "${DATADIR}/warehouse.csv" with csv
\copy DISTRICT from "${DATADIR}/district.csv" with csv
\copy CUSTOMER from "${DATADIR}/customer.csv" with csv
\copy ORDER from "${DATADIR}/order.csv" with csv
\copy ITEM from "${DATADIR}/item.csv" with csv
\copy ORDER_LINE from "${DATADIR}/order-line.csv" with csv
\copy STOCK from "${DATADIR}/stock.csv" with csv
echo $(logtime) "loaded data"
