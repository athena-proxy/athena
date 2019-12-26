#!/bin/bash

bin_dir=`dirname "$0"`
cur_dir=`cd $bin_dir && pwd`
source $bin_dir/global.sh

PROJECT_DIR=`cd $bin_dir;cd ..;pwd`

cd $PROJECT_DIR

retval=0

serverNum=$(getServerNum)

printf "[$0] $appid server is running $serverNum instances\n"
if [[ $serverNum -ge 2 ]];then
    if [[ "X$ELE_URGENCY_RELEASE" != "XON" ]]; then
        retval=-1
        printf "[$0] $appid If want to continue anyway, please config MANUAL_URGENCY=ON in eless\n"
        exit $retval
    fi
fi

printf "[$0] $appid removing athena upgrade file\n"
rm -f $ATHENA_UPGRADE_FILE_PREFIX*

prepareSmoothUpgradeFlagFile
isSuccess=$?
if [[ $isSuccess -ne 0 ]];then
   retval=-1	
fi

exit $retval
