#/bin/bash

root_dir=/home/xp/flying/YCSB-C-all
workload=${root_dir}/workloads/workloada.spec
dbpath=~/OPTANE/ycsb

rmdb() {
	if [ -n "$dbpath" ];then
		echo 'We will delete all files in ' $dbpath
		rm -rf $dbpath/*
	fi
}


if [ $# -lt 1 ]; then
	echo 'Usage : ./xxxx.sh [rocksdb|leveldb|titan] [load|run]'
	exit 0
fi

case $1 in
	'rocksdb')
		case $2 in
			'load')
			    #rmdb
				./ycsbc -db rocksdb -dbpath $dbpath -threads 1 -P $workload -load true
				;;
			'run')
				./ycsbc -db rocksdb -dbpath $dbpath -threads 2 -P $workload -run true
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
				./ycsbc -db leveldb -dbpath $dbpath -threads 1 -P $workload -load true -dboption 1
				;;
			'run')
				./ycsbc -db leveldb -dbpath $dbpath -threads 1 -P $workload -run true -dboption 1
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
				./ycsbc -db titandb -dbpath $dbpath -threads 1 -P $workload -load true -dboption 1
				;;
			'run')
				./ycsbc -db titandb -dbpath $dbpath -threads 1 -P $workload -run true -dboption 1
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
