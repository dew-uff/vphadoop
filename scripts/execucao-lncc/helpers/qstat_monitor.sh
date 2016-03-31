#!/usr/bin/env bash

POLL_TIME=10m

WS=$HOME/qstatmon

curdate=$(date +%d-%m-%Y-%H-%M-%S)

cpwd=$(pwd)

cd $WS

while [ 1 ]; do

    qstat > qstat_$curdate.txt 2>&1

    if [[ "$(grep 'error' qstat_$curdate.txt)" != "" ]]; then
        continue
    fi

    if ! [[ -f qstat_last.txt ]]; then
        ln -s qstat_$curdate.txt qstat_last.txt
        continue
    fi

    rm /tmp/qstat_diff.txt 2> /dev/null
    diff qstat_last.txt qstat_$curdate.txt > /tmp/qstat_diff.txt

    if [[ "$(cat /tmp/qstat_diff.txt)" != "" ]]; then
        mail -s "QSTAT - changed $(date)" gabriel.tessarolli@gmail.com < /tmp/qstat_diff.txt
        rm qstat_last.txt
        ln -s qstat_$curdate.txt qstat_last.txt
    fi

    rm /tmp/qstat_diff.txt

    sleep $POLL_TIME

done

cd $cpwd
