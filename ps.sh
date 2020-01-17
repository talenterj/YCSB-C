#!/bin/sh

# NOTICE: please ensure that you have vmtouch (https://github.com/hoytech/vmtouch)

#use ps to monitor ycsbc cpu, mem, gpu, gpu mem and others
# ' 
#Usage : ./xxxx.sh [process_name] [output_file_name]'
#  example: ./ps.sh compiz stats.output'



# update the path of your KV dbs
dbpath=~/OPTANE/ycsb

pname=$1
# pidof returns nothing if failed
pid=$(pidof $pname)
logtmp=tmp.monitor

if [ "$pid" = "" ] 
then
  ret=""
else
  ret=$(ps --pid $pid | tail -n 1 | awk '{print $1}')
fi


monitoring() {

  #rss: size of used physical ram
  #vms: size of allocated logical ram
  echo "#time cpu rss Gutil Gmem Glat GPWR Gtmp pgrp pid  vms files \$pages RD/s WR/s Rsize Wsize"| tee $logtmp
  echo "#s    %   KB  %     MB   us   Watt C              KB                MB   MB   MB    MB" | tee -a $logtmp

  pargs=$(ps --pid $pid -o 'args' -f | more | tail -n 1)

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
      #pstats2=$(ps --pid $pid -o 'pgrp,pid,uid,user,vsz' | tail -n 1)
      pstats2=$(ps --pid $pid -o 'pgrp,pid,vsz' | tail -n 1)

	  #pcgroup=$(cat /proc/$pid/cgroup |)
  
      #echo "Gutil/% Gtmp/C Gwatt/W Gmem/MB Glat"
      gpu=$(nvidia-smi --query-gpu=utilization.gpu,memory.used,encoder.stats.averageLatency,power.draw,temperature.gpu --format=csv,noheader | tr -d -c '0-9 .')

      #echo "files  cachedPage"
	  pvm=$(vmtouch -f $dbpath | tail -n 4 | awk -F ':' '{print $2}' | xargs | awk '{print $1,$4}')

	  #echo "MB_read/s    MB_wrtn/s    MB_read    MB_wrtn"
	  #use iostat, for nvme device
	  piostat=$(iostat -m | grep nvme | awk '{$1=$2=""; print $0}')

      echo "$pstats1 $gpu $pstats2 $pvm $piostat" | tee -a $logtmp
    else
      # NOT RUNNING
      break
    fi
    sleep 1
  done
  echo "## monitor terminated" | tee -a $logtmp
}


calc() {
  sed -e '/#/d' $logtmp > tmp3 
  
  avgCPU=$(awk '{cpu+=$2}  END{print cpu/NR}' tmp3)
  avgRSS=$(awk '{rss+=$3}  END{print rss/NR}' tmp3)
  avgGPU=$(awk '{gpu+=$4}  END{print gpu/NR}' tmp3)
  avgGMem=$(awk '{gmem+=$5}  END{print gmem/NR}' tmp3)
  rm tmp3
}

refineLog () {
  
  calc
  currTime=$(date +%Y%m%d%H%M%S)

  if [ "$log" = "top.stats" ]
  then
    sed -e '/# wait for process/d' $logtmp > "stats-$currTime".stats
	echo "## cmd line: $pargs" | tee -a "stats-$currTime".stats
    echo "avg CPU : $avgCPU"  | tee -a "stats-$currTime".stats
    echo "avg RSS : $avgRSS"  | tee -a "stats-$currTime".stats
    echo "avg GPU : $avgGPU"  | tee -a "stats-$currTime".stats
    echo "avg GMem: $avgGMem"  | tee -a "stats-$currTime".stats
	echo "logs output file: stats-$currTime.stats"
  else
    sed -e '/# wait for process/d' $logtmp > "$log-$currTime".stats
	echo "## cmd line: $pargs" | tee -a "$log-$currTime".stats
    echo "avg CPU : $avgCPU"  | tee -a "$log-$currTime".stats
    echo "avg RSS : $avgRSS"  | tee -a "$log-$currTime".stats
    echo "avg GPU : $avgGPU"  | tee -a "$log-$currTime".stats
    echo "avg GMem: $avgGMem"  | tee -a "$log-$currTime".stats
	echo "logs output file: $log-$currTime.stats"
  fi

  rm $logtmp
}


trap 'onCtrlC' INT
onCtrlC () {
  refineLog
  exit
}


if [ $# -lt 1 ]; then
    echo 'use ps to monitor ycsbc cpu, mem, gpu, gpu mem and others'A
	echo ' ' 
    echo 'Usage : ./xxxx.sh [process_name] xxx.stats'
    echo '  example: ./ps.sh compiz ps.output'
    exit 0
fi


patient=0
max_patient=360
while [ $patient -lt $max_patient ]; do
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


if [ $patient -eq $max_patient ]
then
  echo "# after $max_patient seconds without sensing your program $1, exit."
else
  # activate monitoring
  if [ "$2" = "" ]
  then
    echo "# seems invalid stats output file name, use default top.stats"
	log="top.stats"
  else
    log=$2
  fi

  monitoring

  refineLog

fi
