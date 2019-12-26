#!/bin/bash
bin_dir=`dirname "$0"`
cur_dir=`cd $bin_dir/start-script && pwd`
source $cur_dir/start-das-global.sh
PROJECT_DIR=`cd $bin_dir;cd ..;pwd`
cd $PROJECT_DIR
CLASS_PATH="$PROJECT_DIR/conf:$PROJECT_DIR/lib/*:$CLASS_PATH"

#register a hook,the int_signal_handler method will be called if the shell process get INT signal
trap "int_signal_handler" INT

echo_t "start to run java,JVM_PLATFORM:$JVM_PLATFORM"
old_java_pid=$(getFirstPid)
#init start opts
gen_start_opts
#must add OnOutOfMemoryError="kill -9 %p" here instead of function gen_start_opts,or else it will start with error
nohup $JAVA_EXE $START_OPTS -XX:OnOutOfMemoryError="kill -9 %p" -classpath $CLASS_PATH $MAIN_CLASS &
java_pid=$!
trySmoothUpgrade
wait "$java_pid"
tryRemoveDalDebugServerFile $java_pid
echo_JVM
exit 1