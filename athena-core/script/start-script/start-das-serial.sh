#!/bin/bash
bin_dir=`dirname "$0"`
cur_dir=`cd $bin_dir/start-script && pwd`
source $cur_dir/start-das-global.sh
PROJECT_DIR=`cd $bin_dir;cd ..;pwd`
cd $PROJECT_DIR
CLASS_PATH="$PROJECT_DIR/conf:$PROJECT_DIR/lib/*:$CLASS_PATH"

function tryDowngradeOldProcess(){
    echo_t "notify the old process to start to downgrade"
    sendCmd2AudsServer $DOWNGRADE_AUDS_PATH $CLOSE_LISTEN_PORTS
    isSuccess=$?
    if [[ $isSuccess -ne 0 ]];then
    	echo_t "fail to send close_listen_ports command to old process!"
    else
    	echo_t "succeed to send close_listen_ports command to old process!"
    fi
    echo_t "wait until downgrade_auds disappear"
    #then it means old process dies.
    isAudsExist $DOWNGRADE_AUDS_PATH
	isDownAudsExist=$?
    while ((!$isDownAudsExist));do
        sleep 1
        isAudsExist $DOWNGRADE_AUDS_PATH
        isDownAudsExist=$?
    done
    tryRemoveDalDebugServerFile $old_java_pid
    echo_t "downgrade old process done."
}

function tryUpgradeNewProcess(){
    echo_t "wait until new process(pid:$java_pid) create upgrade_auds"
    isAudsExist $UPGRADE_AUDS_PATH
    isUpAudsExist=$?
    while (($isUpAudsExist));do
        killAllIfCurrPidExit
        sleep 1
        isAudsExist $UPGRADE_AUDS_PATH
        isUpAudsExist=$?
    done
    echo_t "notify the new process to close upgrade_auds"
    sendCmd2AudsServer $UPGRADE_AUDS_PATH $CLOSE_UPGRADE_UDS
    isSuccess=$?
    if [[ $isSuccess -ne 0 ]];then
    	echo_t "fail to send close_upgrade_uds command to new process!"
    else
    	echo_t "succeed to send close_upgrade_uds command to new process!"
    fi
    echo_t "wait until upgrade_auds disappear"
    isAudsExist $UPGRADE_AUDS_PATH
    isUpAudsExist=$?
    while ((!$isUpAudsExist));do
        killAllIfCurrPidExit
        sleep 1
        isAudsExist $UPGRADE_AUDS_PATH
        isUpAudsExist=$?
    done
    echo_t "upgrade new process done."
}

# serial的不间断升级，利用现有的集群策略，配合goproxy的心跳，实现不间断升级
# 1. 首先降级old process, 关闭与goproxy的心跳链接10s, 并等待old process的退出
# 2. 一般最多6mins后，old process会退出，启动dal new process即可，中间会重新开启与goproxy的心跳

#register a hook,the int_signal_handler method will be called if the shell process get INT signal
trap "int_signal_handler" INT
echo_t "start to run java,JVM_PLATFORM:$JVM_PLATFORM"

tryDowngradeOldProcess
gen_start_opts
#must add OnOutOfMemoryError="kill -9 %p" here instead of function gen_start_opts,or else it will start with error
nohup $JAVA_EXE $START_OPTS -XX:OnOutOfMemoryError="kill -9 %p" -classpath $CLASS_PATH $MAIN_CLASS &
java_pid=$!
tryUpgradeNewProcess
wait "$java_pid"
tryRemoveDalDebugServerFile $java_pid
echo_JVM
exit 1