#!/bin/sh
#
# Download the weekly rucio dump and insert into DB
set -x

dbpass=$1
today=`date +%Y%m%d`
dumpurl=https://rucio-hadoop.cern.ch/repl_factor?date=${today}
dump=/home/dcameron/tmp/dump-${today}
table=dumpx_${today}

wget -O ${dump} ${dumpurl}

create="create table ${table} (scope varchar(255),
                               name varchar(255),
                               length int,
                               size bigint,
                               files_disk_p int,
                               files_disk_s int,
                               files_tape int,
                               size_disk_p bigint,
                               size_disk_s bigint,
                               size_tape bigint,
                               rep_factor_p float,
                               rep_factor_s float,
                               rep_factor_tape float,
                               datatype varchar(255),
                               prod_step varchar(255),
                               run_number int,
                               project varchar(255),
                               stream_name varchar(255),
                               version varchar(255),
                               campaign varchar(255),
                               events bigint)"

mysql -u act -p${dbpass} -e "${create}" rucio_dumps

insert="load data infile '${dump}' into table ${table} fields terminated by '\t' lines terminated by '\n';"

mysql -u act -p${dbpass} -e "${insert}" rucio_dumps

