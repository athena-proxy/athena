#!/bin/bash

bin_dir=`dirname "$0"`
cur_dir=`cd $bin_dir && pwd`
source $bin_dir/global.sh
PROJECT_DIR=`cd $bin_dir;cd ..;pwd`

cd $PROJECT_DIR

mkdir -p "$PROJECT_DIR/tmp/"
mkdir -p "/data/log/${appid}"

# cleaning the gc log and stdout log
function clean_log() {
	log_type=$1
	FILES="/data/log/$appid/$log_type*"
	GCFILES=`ls /data/log/$appid/$log_type* 2> /dev/null|wc -l`

	if [ "X$GCFILES" = "X0" ];then
		return
	fi

	if [ $GCFILES -le 5 ];then
		return
	fi
	DEL_COUNT=$(($GCFILES-5))
	DEL_FILES=`ls -tr /data/log/$appid/$log_type*|head -$DEL_COUNT`
	rm -f $DEL_FILES
}

function checkInitParams(){
    if [ "X$UPGRADE_TYPE" == "Xparallel" ];then
        if [ "X$JVM_PLATFORM" == "XZING" -o "X$JVM_PLATFORM" == "XZGC" ];then
            echo_t "upgrade(type : $UPGRADE_TYPE) not match jvm_platform(platform : $JVM_PLATFORM)"
            return 1
        fi
    fi
    return 0
}

echo_t "#####################################################"
echo_t "#####################################################"
echo_t "#####################################################"
echo_t "#####################################################"

USE_SUPERVISOR=0

if [ "X$1" != "X" ];then
   USE_SUPERVISOR=1
fi

clean_log "gc_"
clean_log "stdout_"

# 检查输入的参数是否合格
checkInitParams
checkResult=$?
if [ $checkResult -eq 1 ];then
     echo "please check huskar config, upgrade(type : $UPGRADE_TYPE) not match jvm_platform(platform : $JVM_PLATFORM)"
     exit 1;
fi

# 1 ----> parallel (parallel execution of two processes)
# 2 ----> mediator (a total of three processes, second process is mediator)
# 3 ----> serial   (serial execution of two processes)
if [ $USE_SUPERVISOR -eq 1 ];then
    if [ "X$UPGRADE_TYPE" == "Xparallel" ];then
        source ./sbin/start-script/start-das-parallel.sh
    elif [ "X$UPGRADE_TYPE" == "Xmediator" ]; then
        source ./sbin/start-script/start-das-mediator.sh
    elif [ "X$UPGRADE_TYPE" == "Xserial" ]; then
        source ./sbin/start-script/start-das-serial.sh
    else
        echo_t "please check huskar config, upgradeType(type : $UPGRADE_TYPE) not exist"
    fi
    exit 1
else
    source ./sbin/start-script/start-das-without-supervisor.sh
fi



