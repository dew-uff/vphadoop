#!/bin/bash
# Gabriel Tessarolli

# $1 - database size
# $2 - where .basex file is located

# if no database size was specified, exit with error
if [ "$1" = "" ]; then
    echo "BaseX (start): should specify db size!"
    exit 1
fi

# where basex data files will be located in the node
BASEX_NODE_DIR=/tmp/gabrielt/basex

# where basex data files are located in the shared dir
DB_ORIGIN_DIR=${HOME}/basex-sunhpc/data_files/$1

# remove any previously existing log file 
rm /tmp/basex-start.log 2> /dev/null

# hosts are in PE_NODEFILE 
# in each node, through ssh, in background (parallel)
# 1) export BASEX_JVM variable remotely, using the BASEX_MEM variable that must be set in job script
# 2) chdir to where .basex file for the running job is located
# 3) try to stop any running server instance
# 4) remove any previous data files that might be still be in the node
# 5) create the remote dir
# 6) copy the new data files to the node dir
# 7) start basex server in service mode
for node in $(cat $PE_NODEFILE); do
    ssh -o StrictHostKeyChecking=no $node "export BASEX_JVM=$BASEX_MEM; cd $2; ${BASEX_HOME}/bin/basexserverstop; rm -rf ${BASEX_NODE_DIR}; mkdir -p ${BASEX_NODE_DIR}; cp -r ${DB_ORIGIN_DIR}/data ${DB_ORIGIN_DIR}/repo ${BASEX_NODE_DIR}; ${BASEX_HOME}/bin/basexserver -S" &
done >> /tmp/basex-start.log 2>&1

# wait all the nodes to complete command above
wait

# print log
cat /tmp/basex-start.log 

# if any ssh access denied appears, we cannot continue.
# job script must check exit code of this script
# anything other than zero means something is wrong and it should abort
if ! [ "$(grep denied /tmp/basex-start.log | wc -l)" = "0" ]; then
    echo "Something wrong while starting BaseX instances!"
    exit 1
else
    echo "BaseX (start): done!"
    exit 0
fi
