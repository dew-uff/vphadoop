#!/bin/bash
# Gabriel Tessarolli

# hosts are in PE_NODEFILE
# $1 is the file to create the database

if [ $# != 1 ]; then
    echo "usage: create-db.sh <file>"
    exit 1
fi

rm -r ${HOME}/.basex
cp ${HOME}/.basex-shared ${HOME}/.basex

tofile=$(basename $1)
fromfile=$1

basex -c"drop db expdb; create db expdb; open expdb; add to ${tofile} ${fromfile}"
