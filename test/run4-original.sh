#! /bin/bash -x
#PJM --rsc-list "node=8"
#PJM --rsc-list "elapse=00:10:00"
#PJM --rsc-list "node-mem=10Gi"
#PJM -s
source /etc/profile.d/modules.sh
export PARALLEL=16
export OMP_NUM_THREADS=$PARALLEL
export fu08bf=1
size="8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152"
#size="256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152"
#size="1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152"
echo  "size time byte/sec"
for i in $size
do
	mpiexec ./pobjtest4-org -len $i
done
