#!/bin/bash
DATADIR='/temp/cs4224a/project_files/data_files'
INSTALLDIR=$HOME/pgsql
SCRIPTSDIR="$HOME/project_files/scripts"
NODE=$(hostname)
COORD=$1

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -f ${SCRIPTSDIR}/schema.sql
echo $(logtime) "created table schemas"
if [ ${NODE} = "$COORD" ]; then
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "SELECT create_distributed_table('warehouse', 'W_ID');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "SELECT create_distributed_table('district', 'D_W_ID');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "SELECT create_distributed_table('customer', 'C_W_ID');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "SELECT create_distributed_table('customer_order', 'O_W_ID');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "SELECT create_reference_table('item');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "SELECT create_distributed_table('order_line', 'OL_W_ID');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "SELECT create_distributed_table('stock', 'S_W_ID');"

    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "\copy WAREHOUSE from "${DATADIR}/warehouse.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "\copy DISTRICT from "${DATADIR}/district.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "\copy CUSTOMER from "${DATADIR}/customer.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "\copy ORDER from "${DATADIR}/order.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "\copy ITEM from "${DATADIR}/item.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "\copy ORDER_LINE from "${DATADIR}/order-line.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d wholesale -c "\copy STOCK from "${DATADIR}/stock.csv" with csv"
    echo $(logtime) "loaded data"
fi
