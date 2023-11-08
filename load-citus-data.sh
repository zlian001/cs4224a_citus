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
echo $(logtime) "node ${NODE}: converting null strings to empty fields in data files" 
# convert 'null' string data to -1 int value so cql query can query empty O_CARRIER_ID value
sed 's/,null,/,-1,/' ${DATADIR}/order.csv > ${DATADIR}/order_null.csv
# convert 'null' strings to empty field `,,`
sed 's/,null,/,,/' ${DATADIR}/order-line.csv > ${DATADIR}/order-line_null.csv
if [ ${NODE} = "$COORD" ]; then
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('warehouse', 'W_ID');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('district', 'D_W_ID');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('customer', 'C_W_ID');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('customer_order', 'O_W_ID');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_reference_table('item');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('order_line', 'OL_W_ID');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('stock', 'S_W_ID');"

    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy WAREHOUSE from "${DATADIR}/warehouse.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy DISTRICT from "${DATADIR}/district.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy CUSTOMER from "${DATADIR}/customer.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy CUSTOMER_ORDER from "${DATADIR}/order.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy ITEM from "${DATADIR}/item.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy ORDER_LINE from "${DATADIR}/order-line.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy STOCK from "${DATADIR}/stock.csv" with csv"
    echo $(logtime) "loaded data"
fi
