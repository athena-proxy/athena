#!/usr/bin/env bash
bin_dir=`dirname "$0"`
cur_dir=`cd $bin_dir/start-script && pwd`
source $bin_dir/global.sh
PROJECT_DIR=`cd $bin_dir;cd ..;pwd`
cd $PROJECT_DIR

LOCAL_IP=$(/sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"|head -1)
java_pid=0
old_java_pid=0
script_name=$0
dt=`date +"%Y%m%d-%H%M%S"`
START_OPTS=""

function trySmoothUpgrade() {
    echo_t "wait until new process(pid:$java_pid) create upgrade_auds"
    isAudsExist $UPGRADE_AUDS_PATH
    isUpAudsExist=$?
    while (($isUpAudsExist));do
        killAllIfCurrPidExit
        sleep 1
        isAudsExist $UPGRADE_AUDS_PATH
        isUpAudsExist=$?
    done
    echo_t "notify the old process to start to downgrade"
    sendCmd2AudsServer $DOWNGRADE_AUDS_PATH $CLOSE_LISTEN_PORTS
    isSuccess=$?
    if [[ $isSuccess -ne 0 ]];then
    	echo_t "fail to send close_listen_ports command to old process!"
    else
    	echo_t "succeed to send close_listen_ports command to old process!"
    fi
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
    echo_t "upgrade_auds has disappeared"
    echo_t "wait until downgrade_auds disappear"
    #then it means old process dies.
    isAudsExist $DOWNGRADE_AUDS_PATH
	isDownAudsExist=$?
    while ((!$isDownAudsExist));do
        killAllIfCurrPidExit
        sleep 1
        isAudsExist $DOWNGRADE_AUDS_PATH
        isDownAudsExist=$?
    done
    echo_t "downgrade old process(pid:$old_java_pid) $old_java_pid done"
    tryRemoveDalDebugServerFile $old_java_pid
    echo_t "upgrade new process(pid:$java_pid) done."
}


function killAllIfCurrPidExit() {
    currExist=1
    for ((i=1;i<4;i++))
    do
        isCurrPidExist $java_pid
        currExist=$?
        if [[ $currExist -eq 0 ]];then
            break
        else
        	echo_t "detect that the process(pid:$java_pid) dies,try the "$i"th detection"
        	sleep 2
        fi
    done

    if [[ $currExist -ne 0 ]];then
       echo_t "new process(pid:$java_pid) dies,kill all the processes with same appid:$appid"
       killAllPids
       echo_t "kill the process(pid:$java_pid) in case that it is still alive when grep failes."
       kill -9 $java_pid
       tryRemoveDalDebugServerFile $java_pid
       exit 1
    fi
}

function gen_start_opts() {
    dt=`date +"%Y%m%d-%H%M%S"`
    START_OPTS="-server -DAPPID=$appid  -DHOSTNAME=`hostname`"
    START_OPTS="$START_OPTS -Dio.netty.threadLocalDirectBufferSize=${threadLocalDirectBufferSize}"
    START_OPTS="$START_OPTS -Djava.io.tmpdir=$PROJECT_DIR/tmp/"
    START_OPTS="$START_OPTS -Duser.dir=$PROJECT_DIR"
    START_OPTS="$START_OPTS -Delog.config=$PROJECT_DIR/conf/logback.xml"
    START_OPTS="$START_OPTS -Djava.rmi.server.hostname=${LOCAL_IP}"
    START_OPTS="$START_OPTS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/heapdump_${appid}_${LOCAL_IP}.hprof"

    reusePort=$(isReuseportSupported)
    if [[ $reusePort -eq 1 ]]; then
       printf "[$script_name] $appid starts with reuseport support\n"
       START_OPTS="$START_OPTS -Dreuseport_support=yes"
    else
       printf "[$script_name] $appid starts with NO reuseport support\n"
       START_OPTS="$START_OPTS -Dreuseport_support=no"
    fi

    if [ "X$MANUAL_RELEASE" == "XON" ];then
       printf "[$script_name] $appid manual release\n"
       START_OPTS="$START_OPTS -Dmanual_release=yes"
    else
       printf "[$script_name] $appid normal release\n"
       START_OPTS="$START_OPTS -Dmanual_release=no"
    fi

    # 选择JVM 平台
    chooseJVM $JVM_PLATFORM

    if [[ "X$ZING_MEM_INFO" == "X" ]]; then
        ZING_MEM_INFO="-Xms1024m -Xmx1024m -Xmn320m"
    fi
    if [[ "X$MEM_INFO" == "X" ]]; then
        MEM_INFO="-Xms1024m -Xmx1024m -Xmn320m"
    fi
    if [ "X$JVM_PLATFORM" == "XZING" ];then
        # 如果hotspot的参数不支持,将会有warn日志
        START_OPTS="$START_OPTS $ZING_MEM_INFO -Xnativevmflags:warn"
        # Ensure the ulimit is unlimited for virtual memory,resident memory,and core file size
        ulimit -v unlimited -m unlimited -c unlimited
    elif [ "X$JVM_PLATFORM" == "XZGC" ];then
         # ZGC
         START_OPTS="$START_OPTS $MEM_INFO -XX:+UnlockExperimentalVMOptions -XX:+UseZGC"
    else
         # G1
         # optimize for the usage of 12-CPU
         START_OPTS="$START_OPTS $MEM_INFO -XX:+UnlockExperimentalVMOptions -XX:+UseG1GC -XX:MaxGCPauseMillis=20  -XX:ParallelGCThreads=12 -XX:ConcGCThreads=${ConcGCThreads} -XX:InitiatingHeapOccupancyPercent=36"
    fi

    # GC Log Enable/Disbale
    if [ "X$ENABLE_GCLOG" == "X1" ];then
        START_OPTS="$START_OPTS -Xloggc:/data/log/${appid}/gc_$dt.log -XX:+PrintGCDetails"
    fi

    # additional jvm parameter
    if [ "X$ADD_PARAM" != "X" ];then
        START_OPTS="$START_OPTS ${ADD_PARAM}"
    fi
}

function send_signal_to_java() {
    if [[ $java_pid -eq 0 ]]; then
        echo_t "NO java pid!!"
        return 1
    fi

    kill -12 $java_pid
    return 0
}

function int_signal_handler() {
    send_signal_to_java
    status=$?

    if [[ $status -eq 0 ]]; then
        echo_t "signal successully sent"
    else
        echo_t "failed to send INT signal to java"
    fi
}


function echo_JVM() {
    if [ "X$JAVA_EXE" == "X$JAVA_HOTSPOT_EXE" ];then
        echo_t "DAL start successfully with Hotspot"
    elif [ "X$JAVA_EXE" == "X$JAVA_ZING_EXE" ];then
        echo_t "DAL start successfully with Zing"
    fi
}