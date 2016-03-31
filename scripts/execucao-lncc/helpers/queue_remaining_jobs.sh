#!/usr/bin/env bash

# Primeiro, os jobs prioritários, necessários para concluir a análise de fragmentação

# NEWPARTIX
cd ~/ws-newpartix
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=60,queries="15" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=60,queries="13" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=60,queries="14" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=120,queries="15" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=120,queries="9" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=120,queries="13" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=120,queries="14" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=120,queries="2" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=30,queries="15" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=30,queries="9" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=30,queries="13" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=30,queries="14" newpartix.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,nfragments=30,queries="2" newpartix.job
sleep 1s

# VPHADOOP
cd ~/ws-vphadoop
qsub -pe hadoop 32 -v dbsize=1GB,ntasks=8,nruns=3,dbtype=basex,queries="15" vphadoop-fragmentation-1-2-4-8.job
sleep 1s
qsub -pe hadoop 32 -v dbsize=1GB,ntasks=8,nruns=3,dbtype=basex,queries="2" vphadoop-fragmentation-1-2-4-8.job
sleep 1s
qsub -pe hadoop 32 -v dbsize=1GB,ntasks=8,nruns=3,dbtype=basex,queries="9" vphadoop-fragmentation-1-2-4-8.job
sleep 1s
qsub -pe hadoop 32 -v dbsize=1GB,ntasks=8,nruns=3,dbtype=basex,queries="13" vphadoop-fragmentation-1-2-4-8.job
sleep 1s
qsub -pe hadoop 32 -v dbsize=1GB,ntasks=8,nruns=3,dbtype=basex,queries="14" vphadoop-fragmentation-1-2-4-8.job
sleep 1s

cd ~/ws-stdalone2
qsub dbsize=10GB_MD,nruns=3,dbtype=basex,queries="14" stdalone.job
sleep 1s

cd ~/ws-partixvp
qsub -pe hadoop 16 -v dbsize=1GB,nthreads=8,nruns=3,queries="15" partixvp.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=10GB,nthreads=8,nruns=3,queries="15" partixvp.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB_MD,nthreads=8,nruns=3,queries="15" partixvp.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=10GB_MD,nthreads=8,nruns=3,queries="15" partixvp.job
sleep 1s
qsub -pe hadoop 32 -v dbsize=10GB,nthreads=8,nruns=3,queries="12" partixvp.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=10GB_MD,nthreads=8,nruns=3,queries="3 4 5 6 7 8 10 11 12" partixvp.job
sleep 1s
qsub -pe hadoop 64 -v dbsize=1GB_MD,nthreads=8,nruns=3,queries="1 3 4 5 6 7 8 10 11 12" partixvp.job
sleep 1s
qsub -pe hadoop 128 -v dbsize=1GB_MD,nthreads=8,nruns=3,queries="1 3 4 5 6 7 8 10 11 12" partixvp.job
sleep 1s
qsub -pe hadoop 64 -v dbsize=10GB_MD,nthreads=8,nruns=3,queries="1 3 4 5 6 7 8 10 11 12" partixvp.job
sleep 1s
qsub -pe hadoop 128 -v dbsize=10GB_MD,nthreads=8,nruns=3,queries="1 3 4 5 6 7 8 10 11 12" partixvp.job
sleep 1s
qsub -pe hadoop 16 -v dbsize=1GB_MD,nthreads=8,nruns=3,queries="12 2 9 13 14" partixvp.job
sleep 1s
qsub -pe hadoop 32 -v dbsize=1GB_MD,nthreads=8,nruns=3,queries="2 9 13 14" partixvp.job
sleep 1s
qsub -pe hadoop 64 -v dbsize=1GB_MD,nthreads=8,nruns=3,queries="2 9 13 14" partixvp.job
sleep 1s
qsub -pe hadoop 128 -v dbsize=1GB_MD,nthreads=8,nruns=3,queries="2 9 13 14" partixvp.job
sleep 1s
qsub -pe hadoop 128 -v dbsize=1GB,nthreads=8,nruns=3,queries="2 9 13 14" partixvp.job
sleep 1s
qsub -pe hadoop 64 -v dbsize=1GB,nthreads=8,nruns=3,queries="13 14" partixvp.job
sleep 1s
qsub -pe hadoop 32 -v dbsize=1GB,nthreads=8,nruns=3,queries="13 14" partixvp.job
sleep 1s
qsub -pe hadoop 32 -v dbsize=10GB_MD,nthreads=8,nruns=3,queries="3 4 5 6 7 8 10 11 12" partixvp.job
sleep 1s