#!/bin/bash

# workdir=/data/$USER/$PBS_JOBID
workdir=/data/$2/$1

if [ -n "$1" ] && [ -d $workdir ]; then
    echo ----------------------------------------
    echo Begin $4 epilogue $(date '+%d/%m/%Y %H:%M:%S')
    echo Cleaning up workdir: $workdir
    rm -rf $workdir
    echo End $4 epilogue $(date '+%d/%m/%Y %H:%M:%S')
    echo ----------------------------------------
fi
