#!/bin/bash
# Gabriel Tessarolli

# in each node, through ssh, in background (parallel)
# 1) chdir to the place where .basex file for the running job is located
# 2) execute drop command with basexclient
#
# wait for all ssh commands to finish
#
# OBS: ssh must be configured as passwordless
#
for node in $(cat $PE_NODEFILE); do
    echo "BaseX (drop tmp) $node: deleting tmp collection..."
    ssh -o StrictHostKeyChecking=no $node "cd $1; basexclient -Uadmin -Padmin -c\"drop db tmpResultadosParciais\" && echo \"\$(hostname) done.\"" &
done
wait
echo "BaseX (drop tmp): done!"
