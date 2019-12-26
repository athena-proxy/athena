#!/bin/bash
bin_dir=`dirname "$0"`
cur_dir=`cd $bin_dir/start-script && pwd`
source $cur_dir/start-das-global.sh
PROJECT_DIR=`cd $bin_dir;cd ..;pwd`
cd $PROJECT_DIR
CLASS_PATH="$PROJECT_DIR/conf:$PROJECT_DIR/lib/*:$CLASS_PATH"

# 三阶段的不间断升级此脚本会被调用两次，共涉及三个进程：old process、mediator、new process。 适用于内存不充足的情况
# 1.会通过判断当前启动的process是否有mediator字样，来启动HOTSPOT, 此HOTSPOT被叫mediator，等待old process downgrade,mediator upgrade
# 2.old downgrade结束后，发出kill -12的命令，此脚本会异常退出，所以supervisor会再次启动start.sh脚本。
# 3.新脚本启动后，依然判断mediator，应启动JVM_PLATFORM对应的，一般为ZING或ZGC, 等待 mediator downgrade， new process upgrade
# 4.new process 升级成功后，wait当前new process 的 pid, 等待kill命令

#register a hook,the int_signal_handler method will be called if the shell process get INT signal
trap "int_signal_handler" INT


old_java_pid=$(getFirstPid)
reusePort=$(isReuseportSupported)
isNeedStartMediator
useMediator=$?

if [ $reusePort -eq 1 -a $useMediator -eq 0 ];then
    JVM_PLATFORM="HOTSPOT"
fi

echo_t "start to run java,JVM_PLATFORM:$JVM_PLATFORM"
#init start opts
gen_start_opts

if [ $reusePort -eq 1 -a $useMediator -eq 0 ];then
    START_OPTS="$START_OPTS -Drole=mediator"
fi

#must add OnOutOfMemoryError="kill -9 %p" here instead of function gen_start_opts,or else it will start with error
nohup $JAVA_EXE $START_OPTS -XX:OnOutOfMemoryError="kill -9 %p" -classpath $CLASS_PATH $MAIN_CLASS &
java_pid=$!
trySmoothUpgrade
isCurrPidMediator $java_pid
isMediator=$?
if [[ $isMediator -eq 0 ]];then
    echo_t "current new process(pid:$java_pid) is mediator,prepare and exit for the next upgrade procedure."
    prepareSmoothUpgradeFlagFile
    int_signal_handler
else
    wait "$java_pid"
fi
tryRemoveDalDebugServerFile $java_pid
echo_JVM
exit 1