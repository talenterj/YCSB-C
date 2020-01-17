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
  
  patient=30
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
				if [ ! -f "hdr/rocksdb-lat.hgrm" ]; then
				  rm "hdr/rocksdb-lat.hgrm"
				fi
				if [ ! -f "hdr/rocksdb-lat.hiccup" ]; then
				  rm "hdr/rocksdb-lat.hiccup"
				fi
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
				if [ ! -f "hdr/cuda-lat.hgrm" ]; then
				  rm "hdr/cuda-lat.hgrm"
				fi
				if [ ! -f "hdr/cuda-lat.hiccup" ]; then
				  rm "hdr/cuda-lat.hiccup"
				fi
				if [ ! -f "hdr/leveldb-lat.hgrm" ]; then
				  rm "hdr/leveldb-lat.hgrm"
				fi
				if [ ! -f "hdr/leveldb-lat.hiccup" ]; then
				  rm "hdr/leveldb-lat.hiccup"
				fi
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
				if [ ! -f "hdr/titan-lat.hgrm" ]; then
				  rm "hdr/titan-lat.hgrm"
				fi
				if [ ! -f "hdr/titan-lat.hiccup" ]; then
				  rm "hdr/titan-lat.hiccup"
				fi
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
