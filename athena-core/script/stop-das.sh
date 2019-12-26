#!/bin/bash

bin_dir=`dirname "$0"`
cur_dir=`cd $bin_dir && pwd`
source $bin_dir/global.sh

PROJECT_DIR=`cd $bin_dir;cd ..;pwd`

function echo_t(){
	dt2=`date +"%Y-%m-%d %H:%M:%S"`
	echo "[$dt2]$@"
}

# We are about to kill the processes which share the same appid and MAIN_CLASS.
max_loop=10
for((i=0;i<$max_loop;i++))
do
	pids=$(getPids)
	if [ -z "$pids" ]
	then
		break;
	fi
	
	if [ $i -gt 5 ]
	then
		_9="-9"
	fi
	
	for pid in $pids
	do
		echo_t "try kill process with pid:$pid"
		if ! kill $_9 $pid
		then
			echo_t "failed kill process with pid:$pid"
		else 
			echo_t "succeed kill process with pid:$pid"
		fi
	done
	
	sleep 3
done

if [ $i -eq $max_loop ]
then
	echo_t "failed kill the processes $pids after try $max_loop times"
	exit 1
fi


exit 0

