#!/bin/bash

SCRIPTS_DIR=`dirname "$0"`

##calucate parent directory path
PROJECT_DIR=`cd $SCRIPTS_DIR && pwd`

source "$PROJECT_DIR/sbin/global.sh"

GET_PID_EXE="$PROJECT_DIR/sbin/get-pid.sh"
START_DAS_EXE="$PROJECT_DIR/sbin/start-das.sh"
STOP_DAS_EXE="$PROJECT_DIR/sbin/stop-das.sh"

retval=0

# start the server
start(){
	printf 'Starting the server\n'
	serverNum=$(getServerNum)
	if [[ $serverNum -ge 2 ]]; then
	    echo "serverNum is $serverNum, you can not start a new one"
	    retval=1
	    return
	fi
    sh +x $START_DAS_EXE
    if [ "$?" -eq 0 ] ; then
    	printf 'Done\n'
    	retval=0
    else
    	printf 'The server could not started\n'
    	retval=1
    fi
}
# stop the server
stop(){
	printf 'Stopping the server\n'
    pids=$(getPids)
	if [ -z "$pids" ]
	then
		printf 'process is not running\n'
		retval=0
	else
		sh +x $STOP_DAS_EXE
		if [ "$?" -eq 0 ];then
			retval=0
		else
			retval=1
		fi
	fi

}

# status for server
status(){
  pids=$(getPids)
  if ! [ -z "$pids" ];then
      for pid in $pids
      do
          printf 'server is running: %d\n' "$pid"
      done
  else
      printf 'server is stopped\n'
  fi
}
# dispatch the command
case "$1" in
start)
  start
  ;;
stop)
  stop
  ;;
status)
  status
  ;;  
restart)
  stop
  start
  ;;
hup)
  hup
  ;;
*)
  printf 'Usage: %s {start|stop|restart|status}\n'
  exit 1
  ;;
esac


# exit
exit "$retval"



# END OF FILE
