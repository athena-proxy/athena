#!/bin/bash

function echo_t(){
	dt2=`date +"%Y-%m-%d %H:%M:%S"`
	echo "[$dt2]$@"
}

if [ -z $1 ]
then
	echo_t "Usage: $0 process-pattern" >&2
	exit 1
fi

exc_pids="^$$$"
curr_pid=$$
while [ $curr_pid -gt 0 ]
do
	curr_pid=`ps -fwwp $curr_pid|grep -v PPID|awk '{print $3}'`
	exc_pids="$exc_pids|^$curr_pid$"
done
curr_script=$0
curr_script=${curr_script#.}
curr_script=${curr_script//./\\.}

if [ $# -eq 1 ]
then
  ps -efww|grep "$1"|grep -Ev "grep|$curr_script"|awk '{print $2}'|grep -Ev $exc_pids
else
  firstArg=$1
  shift
  ps -efww|grep "$firstArg "|grep "$*"|grep -Ev "grep|$curr_script"|awk '{print $2}'|grep -Ev $exc_pids

fi







