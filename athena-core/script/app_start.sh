#!/bin/bash

bin_dir=`dirname "$0"`
cur_dir=`cd $bin_dir && pwd`
source $bin_dir/global.sh

PROJECT_DIR=`cd $bin_dir;cd ..;pwd`

cd $PROJECT_DIR

retval=0

serverNum=$(getServerNum)

if [[ "X$ELE_URGENCY_RELEASE" == "XON" ]]; then
    printf "[$0] $appid server is running $serverNum instances\n"
    printf "[$0] $appid ELE_URGENCY_RELEASE is ON !\n"
    printf "[$0] $appid force to kill all process and restart!\n"

    killAllPids
fi

supervisorctl start $appid