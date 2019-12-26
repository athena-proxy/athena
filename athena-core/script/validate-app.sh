#!/bin/bash

bin_dir=`dirname "$0"`
cur_dir=`cd $bin_dir && pwd`
source $bin_dir/global.sh

PROJECT_DIR=`cd $bin_dir;cd ..;pwd`

cd $PROJECT_DIR

if [ "X$MANUAL_URGENCY" == "XOFF" -a "X$UPGRADE_TYPE" == "Xserial" ];then
    sleep $UPGRADE_TIME_WAIT
else
    sleep 5
fi


retval=0

serverNum=$(getServerNum)

if [[ $serverNum -gt 0 ]];then
    printf "[$0] server is running $serverNum instances now\n"
    retval=0
else
    printf "[$0] server is stopped\n"
    retval=1
fi
exit $retval
