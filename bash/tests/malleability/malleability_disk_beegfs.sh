#! /bin/bash
#SBATCH --job-name=hercules_mall    # Job name
#SBATCH --time=01:00:00               # Time limit hrs:min:sec
#SBATCH --output=logs/hercules/%j_hercules.log   # Standard output and error log
#SBATCH --mem=0
##SBATCH --overcommit
##SBATCH --oversubscribe

spack load mpich

first_it=1
echo 0 > ./hercules_num_act_nodes
export UCX_IB_RCACHE_MAX_REGIONS="262144"
# export UCX_IB_RCACHE_MAX_REGIONS="200000"

for num_data_servers in {16,8,4,2,1}; do
# for num_data_servers in {16..16}; do
## To measure how much time it takes to restart.
if [[ $first_it -eq "0" ]];
then
	end_restart_time=`date +%s.%N`
	restart_time=$( echo "$end_restart_time - $start_restart_time" | bc -l )
	echo "[HS5-$num_data_servers] Restart configuration $restart_time sec."
fi

echo "[HS] Servers $num_data_servers"
# 2 gigas y 10 gigas.
# DIRECTORY_NAME="pdfs-200u"
# DIRECTORY_NAME="input_10files"
DIRECTORY_NAME="input_3files"
# DIRECTORY_NAME="input_1files"
#SOURCE=~/envapp/models/wacommplusplus/wacommplusplus/run/
SOURCE=/beegfs/home/javier.garciablas/hercules/bash/tests/malleability/source/
SINK=/beegfs/home/javier.garciablas/hercules/bash/tests/malleability/sink/$DIRECTORY_NAME_$num_data_servers
#HERCULES_CONFIG_PATH=$1
HERCULES_CONFIG_PATH=/beegfs/home/javier.garciablas/hercules/bash/tests/malleability/configurations_files/dettach/hercules_torino_${num_data_servers}server_1client_in_1nodes.conf
# /beegfs/home/javier.garciablas/hercules/bash/tests/malleability/configurations_files/dettach/hercules_torino_8server_1client_in_1nodes.conf
# HERCULES_PATH=/beegfs/home/javier.garciablas/hercules/scripts/hercules
# HERCULES_PRELOAD_PATH=/home/genarog/Documents/UC3M/Codes/hercules/build/tools/libhercules_posix.so
HERCULES_PATH=/beegfs/home/javier.garciablas/hercules/scripts/hercules
HERCULES_PRELOAD_PATH=/beegfs/home/javier.garciablas/hercules/build/tools/libhercules_posix.so

# start servers.
/usr/bin/time -f "[HS1-$num_data_servers] Start servers %E sec." $HERCULES_PATH start -f $HERCULES_CONFIG_PATH 
#-m ./meta_hostfile -d ./data_hostfile

rm $SINK/$DIRECTORY_NAME/*

# set -x
# run commands (app).
export HERCULES_CONF=$HERCULES_CONFIG_PATH
export LD_PRELOAD=$HERCULES_PRELOAD_PATH
/usr/bin/time -f "[HS2-$num_data_servers] Hercules upload %E sec." cp -r $SOURCE$DIRECTORY_NAME /mnt/hercules/
ls -l /mnt/hercules/$DIRECTORY_NAME
echo "cp -r /mnt/hercules/$DIRECTORY_NAME $SINK"
/usr/bin/time -f "[HS3-$num_data_servers] Hercules save %E sec." cp -r /mnt/hercules/$DIRECTORY_NAME $SINK
# /usr/bin/time -f "[HS] Hercules save %E sec." mv -r /mnt/hercules/$DIRECTORY_NAME $SINK
unset LD_PRELOAD
unset HERCULES_CONF

# stop servers.
/usr/bin/time -f "[HS4-$num_data_servers] Hercules stop %E sec." $HERCULES_PATH stop -f $HERCULES_CONFIG_PATH 
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
echo "###############################################"

done
echo "done"
