#!/bin/sh

#use ps to monitor ycsbc cpu, mem, gpu, gpu mem and others'A
# ' 
#Usage : ./xxxx.sh [process_name]'
#  example: ./ps.sh compiz | tee stats.output'


pname=$1
# pidof returns nothing if failed
pid=$(pidof $pname)

if [ "$pid" = "" ] 
then
  ret=""
else
  ret=$(ps --pid $pid | tail -n 1 | awk '{print $1}')
fi


monitor() {

  #rss: size of used physical ram
  #vms: size of allocated logical ram
  echo "#time cpu rss Gutil Gmem Glat GPWR Gtmp pgrp pid   uid  usr  vms args"
  echo "#s    %   KB  %     MB   us   Watt C                         KB" 

  while [ 1 ]; do
    # check this process is alive
    if [ "$ret" = "" ]
	then
	  break
	else
	  ret=$(ps --pid $pid | tail -n 1 | awk '{print $1}')
	fi
  
    if [ $ret = $pid ]
    then
      # RUNNING
      pstats1=$(ps --pid $pid -o 'etimes,pcpu,rsz' | tail -n 1)
      pstats2=$(ps --pid $pid -o 'pgrp,pid,uid,user,vsz,args' -f | more | tail -n 1)
  
      #echo "Gutil/% Gtmp/C Gwatt/W Gmem/MB Glat"
      gpu=$(nvidia-smi --query-gpu=utilization.gpu,memory.used,encoder.stats.averageLatency,power.draw,temperature.gpu --format=csv,noheader | tr -d -c '0-9 .')
  
      echo $pstats1 $gpu $pstats2 
  #    echo "ret   : $ret"
  #    echo "pname : $pname"
  #    echo "pid   : $pid"
  #    echo "pido  : $pido"
  #    echo "pstats: $pstats"
  #    echo "pstats2: $pstats2"
    else
      # NOT RUNNING
      break
    fi
    sleep 1
  done
  echo "## monitor terminated"
}


if [ $# -lt 1 ]; then
    echo 'use ps to monitor ycsbc cpu, mem, gpu, gpu mem and others'A
	echo ' ' 
    echo 'Usage : ./xxxx.sh [process_name]'
    echo '  example: ./ps.sh compiz | tee stats.output'
    exit 0
fi


patient=0
while [ $patient -lt 10 ]; do
  pid=$(pidof $pname)
  if [ "$pid" = "" ]
  then # not start running
    echo "# wait for process $1 starting" $counter
	patient=$(( $patient + 1 ))
    sleep 1
  else
    ret=$(ps --pid $pid | tail -n 1 | awk '{print $1}')
    break
  fi
done


if [ $patient -eq 10 ]
then
  echo "# after 10 seconds without sensing your program $1, exit."
else
  # activate monitoring
  monitor
fi
