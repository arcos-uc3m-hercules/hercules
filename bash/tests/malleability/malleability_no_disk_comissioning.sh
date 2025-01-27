#! /bin/bash
#SBATCH --job-name=herc_mall    # Job name
#SBATCH --time=01:00:00               # Time limit hrs:min:sec
#SBATCH --output=logs/hercules/%j_hercules.log   # Standard output and error log
##SBATCH --nodelist=broadwell-[040-055,063-064]
##SBATCH --mem=0
##SBATCH --exclusive
##SBATCH --overcommit
##SBATCH --oversubscribe

spack load mpich

first_it=1
prev_num_data_servers=0

#export UCX_IB_RCACHE_MAX_REGIONS="1000"
export UCX_IB_RCACHE_MAX_REGIONS="262144"

INIT_NUM_SERVER=1
MAX_NUM_SERVERS=16

sum_started_nodes=$INIT_NUM_SERVER

# set -x
# for num_data_servers in {16,8,4,2,1}; do
for num_data_servers in {1,16}; do
# for num_data_servers in {16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1}; do
# for num_data_servers in {8,7,6,5,4,3,2,1}; do
# for num_data_servers in {2..1}; do
# for num_data_servers in {16,8}; do
echo "Iteration with $num_data_servers data servers."
echo "[HS] Servers $num_data_servers"
## To measure how much time it takes to restart.
if [[ $first_it -eq "0" ]];
then
	end_restart_time=`date +%s.%N`
	restart_time=$( echo "$end_restart_time - $start_restart_time" | bc -l )
	# echo "[HS] Restart configuration $num_data_servers $restart_time sec."

	num_data_servers_to_be_started=$((num_data_servers-prev_num_data_servers))
	sum_started_nodes=$((num_data_servers_to_be_started+sum_started_nodes))
	echo "Starting $num_data_servers_to_be_started servers, prev_num_data_servers=$prev_num_data_servers, num_data_servers=$num_data_servers, sum_started_nodes=$sum_started_nodes, num_data_servers_to_be_started=$num_data_servers_to_be_started"
	
	echo $num_data_servers > ./hercules_num_act_nodes
	echo "cat ./hercules_num_act_nodes"
	cat ./hercules_num_act_nodes

	# get a list of the host where data nodes will be wake up.
	# tail -n $sum_started_nodes /beegfs/home/javier.garciablas/hercules/bash/tests/malleability/data_hostfile | head -n $num_data_servers_to_be_started > ./data2start_hostfile
	head -n $sum_started_nodes /beegfs/home/javier.garciablas/hercules/bash/tests/malleability/data_hostfile | tail -n $num_data_servers_to_be_started > ./data2start_hostfile

	# send a signal to data nodes.
	/usr/bin/time -f "[HS] Hercules start $num_data_servers_to_be_started %E sec." $HERCULES_PATH add -f $HERCULES_CONFIG_PATH -d ./data2start_hostfile
	sleep 10
fi

## Only the first iteartion.
if [[ $first_it -eq "1" ]];
then
	HERCULES_PATH=/beegfs/home/javier.garciablas/hercules/scripts/hercules
	HERCULES_PRELOAD_PATH=/beegfs/home/javier.garciablas/hercules/build/tools/libhercules_posix.so

	SOURCE=/beegfs/home/javier.garciablas/hercules/bash/tests/malleability/source/
	# DIRECTORY_NAME="input_1files"
	DIRECTORY_NAME="input_3files"
	# DIRECTORY_NAME="input_10files"

	HERCULES_CONFIG_PATH=/beegfs/home/javier.garciablas/hercules/bash/tests/malleability/configurations_files/dettach/hercules_torino_${MAX_NUM_SERVERS}server_1client_in_1nodes.conf


	# start servers.
	/usr/bin/time -f "[HS] Start servers $num_data_servers %E sec." $HERCULES_PATH start -f $HERCULES_CONFIG_PATH

	# exit 0
	
	# enviroment variables.
	export HERCULES_CONF=$HERCULES_CONFIG_PATH
	# export LD_PRELOAD=$HERCULES_PRELOAD_PATH
	# export LD_PRELOAD=/beegfs/home/javier.garciablas/hercules/build/tools/libhercules_posix.so

	# Upload data.
	# LD_PRELOAD=/beegfs/home/javier.garciablas/hercules/build/tools/libhercules_posix.so 
	LD_PRELOAD=$HERCULES_PRELOAD_PATH /usr/bin/time -f "[HS] Hercules upload $num_data_servers %E sec." cp -r $SOURCE$DIRECTORY_NAME /mnt/hercules/
fi

# 2 gigas y 10 gigas.
# DIRECTORY_NAME="pdfs-200u"
#DIRECTORY_NAME="input_3files"
#DIRECTORY_NAME="input_1files"
#SOURCE=~/envapp/models/wacommplusplus/wacommplusplus/run/
SINK=/beegfs/home/javier.garciablas/hercules/bash/tests/malleability/sink/$DIRECTORY_NAME_$num_data_servers


# set -x
# run commands (app).

LD_PRELOAD=$HERCULES_PRELOAD_PATH ls -l /mnt/hercules/$DIRECTORY_NAME

# Copy from hercules to file system.
rm $SINK/$DIRECTORY_NAME/*
LD_PRELOAD=$HERCULES_PRELOAD_PATH /usr/bin/time -f "[HS] Hercules save $num_data_servers %E sec." cp -r /mnt/hercules/$DIRECTORY_NAME $SINK

# Verify data consistency.
rm original_checksums.txt hercules_checksums.txt
## echo "diff $SOURCE $SINK"
## diff $SOURCE/$DIRECTORY_NAME $SINK/$DIRECTORY_NAME
echo "Checksums in $SOURCE/$DIRECTORY_NAME/"
md5sum $SOURCE/$DIRECTORY_NAME/* > original_checksums.txt
echo "------------------------------------"
md5sum $SINK/$DIRECTORY_NAME/* > hercules_checksums.txt
echo "------------------------------------"
diff original_checksums.txt hercules_checksums.txt
echo "------------------------------------"

# get dataservers to be stopped.
#-m ./meta_hostfile -d ./data_hostfile

start_restart_time=`date +%s.%N`
#sleep 120
## Time to start the next configuration.
# # start servers with the new configuration.
# /usr/bin/time -f "[HS] Restart servers %E sec." $HERCULES_PATH start -f $HERCULES_CONFIG_PATH 
# #-m ./meta_hostfile -d ./data_hostfile

# # copy prev. backup (sink) to the server.
# export HERCULES_CONF=$HERCULES_CONFIG_PATH
# export LD_PRELOAD=$HERCULES_PRELOAD_PATH
# /usr/bin/time -f "[HS] Hercules restore %E sec." cp -r $SINK$DIRECTORY_NAME /mnt/hercules/$DIRECTORY_NAME
# ls -l /mnt/hercules/$DIRECTORY_NAME
# unset LD_PRELOAD
# unset HERCULES_CONF
# # set +x

# # stops servers.
# /usr/bin/time -f "[HS] Hercules stop %E sec." $HERCULES_PATH stop -f $HERCULES_CONFIG_PATH 
# #-m ./meta_hostfile -d ./data_hostfile
first_it=0
prev_num_data_servers=$num_data_servers
echo "###############################################"
done
echo "Exit from for loop."

unset LD_PRELOAD
unset HERCULES_CONF

# stop reamining servers.
echo 0 > ./hercules_num_act_nodes
cat /beegfs/home/javier.garciablas/hercules/bash/tests/malleability/data_hostfile > ./data2stop_hostfile
/usr/bin/time -f "[HS] Hercules stop $num_data_servers %E sec." $HERCULES_PATH stop -f $HERCULES_CONFIG_PATH -d ./data2stop_hostfile -w 0

echo "done"
