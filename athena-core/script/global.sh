#!/bin/bash

appid="me.ele.arch.das.misc"

MAIN_CLASS="me.ele.jarch.athena.netty.AthenaServer"

export FULL_HOSTNAME=`hostname -f`

MEM_INFO="-Xms512m -Xmx2g"
ConcGCThreads=6
threadLocalDirectBufferSize=0

ZING_MEM_INFO="-Xms512m -Xmx2g"

UPGRADE_TYPE=parallel
UPGRADE_TIME_WAIT=380

ENABLE_GCLOG=1

UPGRADE_PATH="/data/run/dal_upgrade"
ATHENA_UPGRADE_FILE_PREFIX=$UPGRADE_PATH"/"$appid"_athena_smooth_upgrade_flag_"

ZING_BIN_PATH=/opt/zing/zing-jdk1.8.0-16.01.3.0-3-x86_64/bin
JAVA_HOTSPOT_EXE="java"
JAVA_ZING_EXE="$ZING_BIN_PATH/java"
# default hotspot
JAVA_EXE=$JAVA_HOTSPOT_EXE
JVM_PLATFORM="HOTSPOT"

#upgrade unix domain socket path
UPGRADE_AUDS_PATH=$UPGRADE_PATH"/"$appid"_upgrade_auds.sock"
#downgrade unix domain socket path
DOWNGRADE_AUDS_PATH=$UPGRADE_PATH"/"$appid"_downgrade_auds.sock"
#command sent to server
CLOSE_UPGRADE_UDS="close_upgrade_uds"
CLOSE_LISTEN_PORTS="close_listen_ports"

function prepareSmoothUpgradeFlagFile() {
    if [[ "X$ELE_URGENCY_RELEASE" == "XON" ]]; then
        return 0
    fi
    
    mkdir -p -m 777 $UPGRADE_PATH  
    touch $ATHENA_UPGRADE_FILE_PREFIX`date +%s%3N`
    status=$?
    if [[ $status -eq 0 ]]; then
    	return 0
    fi
    printf "[$0] $appid failed to touch upgrade file\n"
    return 1
}

function getPids() {
    # The reason of adding a blank to the end of appid is that some of our APPIDs have
    # prefix relationship, such as "me.ele.arch.das.misc" and "me.ele.arch.das.misc.master".
    # As such, we avoid false kill.
    ps aux | grep "DAPPID=$appid " | grep $MAIN_CLASS | grep -v sudo  | grep -v grep | awk '{print $2}'
}

function getServerNum() {
    pids=$(getPids)
    # The reason for this for-loop is that when there is no server is running,
    # the getPids return one array with an empty element, which makes the
    # ${#pids[@]} equals to 1, which is not what we expect.
    num=0
    for pid in $pids
    do
      num=`expr $num + 1`
    done
    echo $num
}

function getFirstPid() {
    pids=$(getPids)
    isEcho=1
    for pid in $pids
    do
      echo $pid
      isEcho=0
      break
    done
    if [[ $isEcho -ne 0 ]];then
    	echo 0
    fi
}

function killAllPids() {
    pids=$(getPids)
    echo_t "[$0] $appid trying to kill \"$pids\""
    for pid in $pids
    do
        echo_t "[$0] $appid try to kill $pid"
        kill -9 $pid
        rm -f /tmp/dal_debug_server_$(shortAppId)-$pid.txt
    done
}

function isCurrPidExist() {
    pids=$(getPids)
    for pid in $pids
    do 
      if [[ "X$pid" == "X$1" ]];then
         return 0
      fi
    done
    return 1
}

function isAudsExist() {
    auds=`netstat -an |grep "@$1" |grep -v grep | awk '{print $9}'`
    if [[ "X$auds" != "X" ]];then
        return 0
    else
        return 1
    fi
}

function sendCmd2AudsServer() {
    isAudsExist $1
    isExist=$?
    if [[ $isExist -ne 0 ]];then
        echo_t "$1 not exist."
        return 0
    fi
    exec_rs=`echo "$2"|socat - abstract-connect:$1`
    if [[ "X$exec_rs" == "Xexec_sucess" ]];then
    	return 0
    fi
    return 1
}

function isCurrPidMediator() {
    pids=`ps aux | grep "DAPPID=$appid " | grep $MAIN_CLASS | grep "Drole=mediator" | grep -v sudo  | grep -v grep | awk '{print $2}'`
    for pid in $pids
    do
      if [[ "X$1" == "X$pid" ]];then
          return 0
      fi
    done  
    return 1
}

function isNeedStartMediator() {
    isAudsExist $DOWNGRADE_AUDS_PATH
    isDownAdusExist=$?
    # if there is no DOWNGRADE_AUDS_PATH,meanning no previous process, so no need use mediator strategy
    if [ $isDownAdusExist -ne 0 ];then
        return 1
    fi
    pids=`ps aux | grep "DAPPID=$appid " | grep $MAIN_CLASS | grep "Drole=mediator" | grep -v sudo  | grep -v grep | awk '{print $2}'`
    num=0
    for pid in $pids
    do
      num=`expr $num + 1`
    done
    if [ $num -eq 0 ];then
        # should use mediator strategy to stop old process and start the third process
        return 0
    else
        # already have one process using mediator strategy,and make sure there is only one.
        return 1
    fi
}

function tryRemoveDalDebugServerFile() {
    isCurrPidExist $1
    currExist=$?
    if [[ $currExist -ne 0 ]];then
       rm -f /tmp/dal_debug_server_$(shortAppId)-$1.txt
    fi
}

function shortAppId() {
    echo $appid | awk -F'.' '{print $NF}'
}

function echo_t() {
    dt2=`date +"%Y-%m-%d %H:%M:%S"`
    echo "[$dt2]$@"
}

function isReuseportSupported() {
    major=`uname -r | awk -F'.' '{print $1}'`
    minor=`uname -r | awk -F'.' '{print $2}'`
    if [[ $major -gt 3 || $major -eq 3 && $minor -ge 9 ]]; then
        echo 1
    else
        echo 0
    fi
}

function isAvailableJVM() {
    $1 -version > /dev/null 2>&1
    if [ $? -eq 0 ];then
        echo "TRUE"
    else
        echo "FALSE"
    fi
}

# 选择JVM 平台
# 属于优先级选择,如果目标Jvm不可用,使用另外一个
# 都不可用则报错
function chooseJVM() {

    hostspotflag=$(isAvailableJVM $JAVA_HOTSPOT_EXE)
    zingflag=$(isAvailableJVM $JAVA_ZING_EXE)

    if [ "X${hostspotflag}" == "XFALSE" -a "X${zingflag}" == "XFALSE" ];then
        echo_t "no available JVM." && exit 1
    fi
    if [ "X$JVM_PLATFORM" == "XZING" ];then
        JAVA_EXE=$JAVA_ZING_EXE
        if [ "X${zingflag}" == "XFALSE" ];then
            echo_t "Error : not the expect JVM(ZING)." && exit 1
        fi
    elif [ "X$JVM_PLATFORM" == "XZGC" ];then
        JAVA_EXE=$JAVA_HOTSPOT_EXE
        if [ "X${hostspotflag}" == "XFALSE" ];then
            echo_t "Error : not the expect JVM(ZGC)." && exit 1
        fi
    else
        JAVA_EXE=$JAVA_HOTSPOT_EXE
        if [ "X${hostspotflag}" == "XFALSE" ];then
            echo_t "Error : not the expect JVM(HOTSPOT)." && exit 1
        fi
    fi
}

