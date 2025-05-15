#!/bin/bash

SCRIPT_NAME="mdtest_hercules_slurm.sh"
TOTAL_NUMBER_OF_FILES=100000

ATTACHED=1

#TEST_TYPE="weak"
TEST_TYPE="strong"
TEMPLATE_CONFIG_PATH="/beegfs/home/javier.garciablas/hercules/conf/hercules-template.conf"
HERCULES_PATH="/beegfs/home/javier.garciablas/hercules"
HERCULES_CHECKPOINT_PATH=""
#HERCULES_SNAPSHOT_PATH="/beegfs/home/javier.garciablas/hercules/bash/tests/disk/HerculesSnapshot"
DATA_HOSTFILE="/beegfs/home/javier.garciablas/hercules/tmp/data_hostfile"
METADATA_HOSTFILE="/beegfs/home/javier.garciablas/hercules/tmp/meta_hostfile"
DEBUG_LEVEL="none"
#RR, BUCKETS, HASH, CRC16b, CRC64b, LOCAL, ZCOPY
export POLICY="HASH"

NUM_DATA_SERVERS_RANGE=(16)
#NUM_DATA_SERVERS_RANGE=( 1 2 4 8 16 )
NUM_METADATA_SERVERS_RANGE=(1)
#NUM_METADATA_SERVERS_RANGE=( 4 8 16 32 )
NODES_FOR_CLIENTS_RANGE=(16)
# NODES_FOR_CLIENTS_RANGE=(1 2 4 8 16)
#CLIENTS_PER_NODE_RANGE=( 1 2 4 8 16 32 )
CLIENTS_PER_NODE_RANGE=(16)
BLOCK_SIZE_RANGE=(512)
THREAD_POOL=1

MAX_ITERATIONS=1

# set -o xtrace
#set -x

jid=-1

# Checks if template configuration file exists.
if [ ! -f ${TEMPLATE_CONFIG_PATH} ]; then
	echo "File not found: ${TEMPLATE_CONFIG_PATH}"
	exit 0
fi

for test_number in $(seq 1 $MAX_ITERATIONS); do
	for METADATA_NUM_SERVERS in "${NUM_METADATA_SERVERS_RANGE[@]}"; do
		# LOWER_BOUND=$DATA_NUM_SERVERS
		for DATA_NUM_SERVERS in "${NUM_DATA_SERVERS_RANGE[@]}"; do
			for NODES_FOR_CLIENTS in "${NODES_FOR_CLIENTS_RANGE[@]}"; do
				#NODES_FOR_CLIENTS=$DATA_NUM_SERVERS
				for CLIENTS_PER_NODE in "${CLIENTS_PER_NODE_RANGE[@]}"; do
					for BLOCK_SIZE in "${BLOCK_SIZE_RANGE[@]}"; do
						## Calculates the number of nodes to be allocated.
						## +1 because the metadata node.
						if [ $ATTACHED -eq 1 ]; then
							NUMBER_OF_NODES=$((METADATA_NUM_SERVERS + DATA_NUM_SERVERS))
							mode="Attach"
						else
							NUMBER_OF_NODES=$((METADATA_NUM_SERVERS + DATA_NUM_SERVERS + NODES_FOR_CLIENTS))
							mode="Detach"
						fi
						
						TOTAL_NUMBER_OF_CLIENTS=$((NODES_FOR_CLIENTS * CLIENTS_PER_NODE))

						## Calculates the amount of data per client according
						## to the type of test.
						if [ "$TEST_TYPE" = "weak" ]; then
							NUMBER_OF_FILES_PER_PROCESS=${TOTAL_NUMBER_OF_FILES}
						## Strong by default.
						else
							NUMBER_OF_FILES_PER_PROCESS=$((${TOTAL_NUMBER_OF_FILES} / ${TOTAL_NUMBER_OF_CLIENTS}))
						fi

						# Make configuration folder if it does not exists.
						DIRECTORY="./HerculesConfigurations/${mode}/"
						if [ ! -d ${DIRECTORY} ]; then
							echo "Making directory ${DIRECTORY}"
							mkdir -p ${DIRECTORY}
						fi

						CONFIG_PATH="${DIRECTORY}/${METADATA_NUM_SERVERS}m+${DATA_NUM_SERVERS}s-${NODES_FOR_CLIENTS}nfc-${CLIENTS_PER_NODE}cpd_${BLOCK_SIZE}blocksize.conf"

						## If the configuration file does not exist then we create one by using a template and modifying the necessary variables.
						#				if ! [ -f "$CONFIG_PATH" ]; then

						cp $TEMPLATE_CONFIG_PATH $TEMPLATE_CONFIG_PATH."_TEMP"
						## TODO: .* to match all characters in regulars expression.
						sed -i "s/^BLOCK_SIZE = [0-9]*/BLOCK_SIZE = $BLOCK_SIZE/g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s/^NUM_DATA_SERVERS = [0-9]/NUM_DATA_SERVERS = $DATA_NUM_SERVERS/g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s/^NUM_META_SERVERS = [0-9]/NUM_META_SERVERS = $METADATA_NUM_SERVERS/g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s/^NUM_NODES_FOR_CLIENTS = [0-9]/NUM_NODES_FOR_CLIENTS = $NODES_FOR_CLIENTS/g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s/^NUM_CLIENTS_PER_NODE = [0-9]/NUM_CLIENTS_PER_NODE = $CLIENTS_PER_NODE/g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s/^INIT_NUM_DATA_SERVERS = [0-9]/INIT_NUM_DATA_SERVERS = $DATA_NUM_SERVERS/g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s/^ATTACHED = [0-9]/ATTACHED = $ATTACHED/g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^POLICY = .*|POLICY = $POLICY|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^HERCULES_PATH = .*|HERCULES_PATH = $HERCULES_PATH|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^HERCULES_CHECKPOINT_PATH = .*|HERCULES_CHECKPOINT_PATH = $HERCULES_CHECKPOINT_PATH|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^HERCULES_SNAPSHOT_PATH = .*|HERCULES_SNAPSHOT_PATH = $HERCULES_SNAPSHOT_PATH|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^DATA_HOSTFILE = .*|DATA_HOSTFILE = $DATA_HOSTFILE|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^METADATA_HOSTFILE = .*|METADATA_HOSTFILE = $METADATA_HOSTFILE|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^DEBUG_LEVEL = .*|DEBUG_LEVEL = $DEBUG_LEVEL|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^THREAD_POOL = .*|THREAD_POOL = $THREAD_POOL|g" "$TEMPLATE_CONFIG_PATH"

						cat $TEMPLATE_CONFIG_PATH >"$CONFIG_PATH"
						cp $TEMPLATE_CONFIG_PATH."_TEMP" $TEMPLATE_CONFIG_PATH
						#				fi
						echo "${CONFIG_PATH}"
						#continue	## FIXED
						# exit 0
						set -x

						## The first job does not have dependencie (do not wait for another job to end).
						# if [ "$jid" -eq 1 ]; then
						# 	jid=$(sbatch -N $NUMBER_OF_NODES $SCRIPT_NAME "$CONFIG_PATH" "$FILE_SIZE_PER_CLIENT" "$TOTAL_NUMBER_OF_CLIENTS" "$CLIENTS_PER_NODE" "$IOR_FILE_PER_PROCESS" "$IOR_AVOID_CACHE" | cut -d ' ' -f4)
						# ## The following jobs wait for the previous job to finish.
						# else
							jid=$(sbatch --dependency=afterany:"${jid}" -N $NUMBER_OF_NODES $SCRIPT_NAME "$CONFIG_PATH" "$NUMBER_OF_FILES_PER_PROCESS" "$TOTAL_NUMBER_OF_CLIENTS" "$CLIENTS_PER_NODE" | cut -d ' ' -f4)
						# fi
						echo $jid
						### exit 0
						set +x

					done
				done
			done
		done
	done
done

set +o xtrace
