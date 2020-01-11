#/bin/bash

root_dir=/home/xp/flying/YCSB-C-all
workload=${root_dir}/workloads/workloada.spec
dbpath=~/OPTANE/ycsb

rmdb() {
    rm -fr compactInfo.output
	if [ -n "$dbpath" ];then
		echo 'We will delete all files in ' $dbpath
		rm -rf $dbpath/*
	fi
}


if [ $# -lt 1 ]; then
	echo 'Usage : ./xxxx.sh [rocksdb|leveldb|titan] [load|run]'
	exit 0
fi


chill_optane() {
  echo "# sleep 60s to avoid possible OPTANE in-drive cache effects."
  echo "# you could avoid this by commenting there in test_leveldb.sh."
  
  patient=60
  while [ $patient -gt 0 ]; do
    echo "# $patient seconds left to start $1" 
    patient=$(( $patient - 1 ))
    sleep 1
  done
}


case $1 in
	'rocksdb')
		case $2 in
			'load')
			    #rmdb
				./ycsbc -db rocksdb -dbpath $dbpath -threads 1 -P $workload -load true
				;;
			'run')
			    chill_optane
				./ycsbc -db rocksdb -dbpath $dbpath -threads 4 -P $workload -run true
				;;
			*)
				echo 'Error INPUT'
				exit 0
				;;
		esac
		;;
	'leveldb')
		case $2 in
			'load')
			    rmdb
				./ycsbc -db leveldb -dbpath $dbpath -threads 1 -P $workload -load true
				;;
			'run')
			    chill_optane
				./ycsbc -db leveldb -dbpath $dbpath -threads 1 -P $workload -run true
				;;
			*)
				echo 'Error INPUT'
				exit 0
				;;
		esac
		;;
	'titan')
		case $2 in
			'load')
			    rmdb
				./ycsbc -db titandb -dbpath $dbpath -threads 1 -P $workload -load true
				;;
			'run')
			    chill_optane
				./ycsbc -db titandb -dbpath $dbpath -threads 4 -P $workload -run true
				;;
			*)
				echo 'Error INPUT'
				exit 0
				;;
		esac
		;;
	*)
		echo "Error INPUT"
		exit 0
		;;
esac
