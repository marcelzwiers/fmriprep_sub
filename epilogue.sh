#!/bin/bash

# workdir=/data/$USER/$PBS_JOBID
workdir=/data/$2/$1

if [ -n "$1" ] && [ -d $workdir ]; then
    echo ----------------------------------------
    echo Begin PBS Epilogue $(date '+%d/%m/%Y %H:%M:%S')
    echo cleaning up workdir $workdir
    rm -rf $workdir
    echo End PBS Epilogue $(date '+%d/%m/%Y %H:%M:%S')
    echo ----------------------------------------
fi
