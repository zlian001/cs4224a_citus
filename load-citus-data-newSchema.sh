#!/bin/bash
DATADIR='/temp/cs4224a/project_files/data_files'
INSTALLDIR=$HOME/pgsql
SCRIPTSDIR="$HOME/project_files/scripts"
NODE=$(hostname)
COORD=$1

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

echo $(logtime) "created table schemas"
if [ ${NODE} == "$COORD" ]; then
    echo $(logtime) "node ${NODE}: creating tables"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -f ${SCRIPTSDIR}/schema.sql

    echo $(logtime) "node ${NODE}: converting null strings to empty fields in data files" 
    # convert 'null' string data to -1 int value so cql query can query empty O_CARRIER_ID value
    sed 's/,null,/,-1,/' ${DATADIR}/order.csv > ${DATADIR}/order_null.csv
    # convert 'null' strings to empty field `,,`
    sed 's/,null,/,,/' ${DATADIR}/order-line.csv > ${DATADIR}/order-line_null.csv

    sleep 120
    echo $(logtime) "node ${NODE}: distributing tables"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_reference_table('warehouse');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('district', 'd_id');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('customer', 'c_d_id');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('customer_order', 'o_d_id');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('item', 'i_id');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('order_line', 'o_d_id');"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "SELECT create_distributed_table('stock', 'o_i_id');"

    echo $(logtime) "node ${NODE}: creating foreign keys" # can only do so after distribution
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "ALTER TABLE order_line ADD FOREIGN KEY (OL_I_ID) REFERENCES item(I_ID);"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "ALTER TABLE stock ADD FOREIGN KEY (S_I_ID) REFERENCES item(I_ID);"

    echo $(logtime) "node ${NODE}: loading data"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy WAREHOUSE from "${DATADIR}/warehouse.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy DISTRICT from "${DATADIR}/district.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy CUSTOMER from "${DATADIR}/customer.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy CUSTOMER_ORDER from "${DATADIR}/order_null.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy ITEM from "${DATADIR}/item.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy ORDER_LINE from "${DATADIR}/order-line_null.csv" with csv"
    ${INSTALLDIR}/bin/psql -U cs4224a -d $PGDATABASE -c "\copy STOCK from "${DATADIR}/stock.csv" with csv"
fi
