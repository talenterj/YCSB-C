#!/bin/bash

# randomly generate CPU load for a short moment every 10s


random_from_range() {
    min=1
    max=50
    mid=$(($max-$min+1))
    num=$(head -n 20 /dev/urandom | cksum | cut -f1 -d ' ')
    randnum=$(($num%$mid+$min))        
    echo $randnum
}


trap 'onCtrlC' INT
onCtrlC () {
  echo "# CPU load generator exit."
  exit
}


usage() {
  echo "Usage:"
  echo "  ./cpu-load.sh [load level]"
  echo ""
  echo "Use stress-ng randomly generate CPU load for a"
  echo "short moment (0 to 0.5s) every 10s."
}


while true
do
  randslice=$(random_from_range)
  if [ "$1" = "" ]
  then
    echo "set default CPU load level 20"
	level=20
  else
    level=$1
  fi
  tmp=$(stress-ng -c 0 --cpu-load-slice $randslice -l $level -t 1)
  echo "stress-ng -c 0 --cpu-load-slice $randslice -l $level -t 1"
  sleep 10
done
