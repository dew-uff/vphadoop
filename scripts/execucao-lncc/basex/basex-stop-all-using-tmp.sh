#!/bin/bash
# Gabriel Tessarolli

# hosts are in PE_NODEFILE

# where to copy basex log files for this job. they are important for debugging
LOG_DB_OUTPUT_DIR=${HOME}/basex-sunhpc/logs/${JOB_ID}
mkdir -p ${LOG_DB_OUTPUT_DIR}

# for each node, 
# 1) stop the server service
# 2) copy log dir to specific dir in the shared dir
# 3) delete the data file directory
for node in $(cat $PE_NODEFILE); do
    echo "BaseX (stop): $node: Stopping server and copying log files to ${LOG_DB_OUTPUT_DIR}/log-$node ..."
    ssh -o StrictHostKeyChecking=no $node "cd $1; ${BASEX_HOME}/bin/basexserverstop; cp -r /tmp/gabrielt/basex/data/.logs ${LOG_DB_OUTPUT_DIR}/log-\$(hostname); rm -rf /tmp/gabrielt/basex" &
done
wait
echo "BaseX (stop): done!"
