#!/bin/bash

SBATCH_FLAGS="--partition=large -A uc3m_a0-sciot" # -- exclusiv # -- exclusive
SCRIPT_NAME="c3_hercules_ior_slurm.sh"
# FILE_SIZE=$((1024*1024*100))
FILE_SIZE=$((1024*1024*10))

ATTACHED=0

# 0 = Shared single file, 1 = File per process.
IOR_FILE_PER_PROCESS=1
# 0 = Do not avoid cache, 1 = Avoid cache (recommend by https://ior.readthedocs.io/en/latest/userDoc/tutorial.html).
IOR_AVOID_CACHE=0

#TEST_TYPE="weak"
TEST_TYPE="strong"
HERCULES_PATH="/home/tester004/gesanche/hercules"
TEMPLATE_CONFIG_PATH="${HERCULES_PATH}/conf/hercules-template.conf"
HERCULES_CHECKPOINT_PATH=""
#HERCULES_SNAPSHOT_PATH="${HERCULES_PATH}/bash/tests/disk/HerculesSnapshot"
DATA_HOSTFILE="${HERCULES_PATH}/tmp/data_hostfile"
ALLOC_DATA_HOSTFILE="${HERCULES_PATH}/tmp/alloc_data_hostfile"
METADATA_HOSTFILE="${HERCULES_PATH}/tmp/meta_hostfile"
DEBUG_LEVEL="none"
#RR, BUCKETS, HASH, CRC16b, CRC64b, LOCAL, ZCOPY
export POLICY="RR"

#MAX_DATA_SERVERS_RANGE=(16)
MALLEABILITY=1
MALLEABILITY_TOLERANCE=100
MALLEABILITY_WSIZE=20
MALLEABILITY_THRESHOLD=200 # MB
MAX_DATA_SERVERS_RANGE=(16)
NUM_DATA_SERVERS_RANGE=(1)
#NUM_DATA_SERVERS_RANGE=( 1 2 4 8 16 )
NUM_METADATA_SERVERS_RANGE=(1)
#NUM_METADATA_SERVERS_RANGE=( 4 8 16 32 )
NODES_FOR_CLIENTS_RANGE=(1)
# NODES_FOR_CLIENTS_RANGE=(1 2 4 8 16)
#CLIENTS_PER_NODE_RANGE=( 1 2 4 8 16 32 )
CLIENTS_PER_NODE_RANGE=(16)
BLOCK_SIZE_RANGE=( 512 )
THREAD_POOL=1
STORAGE_SIZE=0
PREFETCH_SIZE=0 #2048

MAX_ITERATIONS=1

# set -o xtrace
#set -x

jid=-1

# Checks if template configuration file exists.
if [ ! -f ${TEMPLATE_CONFIG_PATH} ]; then
	echo "File not found: ${TEMPLATE_CONFIG_PATH}"
	exit 0
fi

if [ $MALLEABILITY -eq 0 ]; then
	MAX_DATA_SERVERS_RANGE=${NUM_DATA_SERVERS_RANGE}
fi

for test_number in $(seq 1 $MAX_ITERATIONS); do
	for METADATA_NUM_SERVERS in "${NUM_METADATA_SERVERS_RANGE[@]}"; do
		# LOWER_BOUND=$DATA_NUM_SERVERS
		for DATA_NUM_SERVERS in "${NUM_DATA_SERVERS_RANGE[@]}"; do
			for NODES_FOR_CLIENTS in "${NODES_FOR_CLIENTS_RANGE[@]}"; do
				for CLIENTS_PER_NODE in "${CLIENTS_PER_NODE_RANGE[@]}"; do
					for BLOCK_SIZE in "${BLOCK_SIZE_RANGE[@]}"; do
						## Calculates the number of nodes to be allocated.
						## +1 because the metadata node.
						if [ $ATTACHED -eq 1 ]; then
							# NUMBER_OF_NODES=$((METADATA_NUM_SERVERS + DATA_NUM_SERVERS))
							NUMBER_OF_NODES=$((METADATA_NUM_SERVERS + MAX_DATA_SERVERS_RANGE))
							mode="Attach"
						else
							# NUMBER_OF_NODES=$((METADATA_NUM_SERVERS + DATA_NUM_SERVERS + NODES_FOR_CLIENTS))
							NUMBER_OF_NODES=$((METADATA_NUM_SERVERS + MAX_DATA_SERVERS_RANGE + NODES_FOR_CLIENTS))
							mode="Detach"
						fi
						
						TOTAL_NUMBER_OF_CLIENTS=$((NODES_FOR_CLIENTS * CLIENTS_PER_NODE))

						## Calculates the amount of data per client according 
						## to the type of test. 
						if [ "$TEST_TYPE" = "weak" ]; then
							FILE_SIZE_PER_CLIENT=$((1024*1024))
						## Strong by default.
						else
							# TOTAL_NUMBER_OF_CLIENTS=$((NODES_FOR_CLIENTS * CLIENTS_PER_NODE))
							FILE_SIZE_PER_CLIENT=$((FILE_SIZE/TOTAL_NUMBER_OF_CLIENTS))
						fi

						# Make configuration folder if it does not exists.
						DIRECTORY="./configurations_hercules/${mode}/"
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
						sed -i "s/^MALLEABILITY = [0-9]/MALLEABILITY = $MALLEABILITY/g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s/^UPPER_BOUND_MALLEABILITY = [0-9]/UPPER_BOUND_MALLEABILITY = $MAX_DATA_SERVERS_RANGE/g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^MALLEABILITY_TOLERANCE = .*|MALLEABILITY_TOLERANCE = $MALLEABILITY_TOLERANCE|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^MALLEABILITY_WSIZE = .*|MALLEABILITY_WSIZE = $MALLEABILITY_WSIZE|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^MALLEABILITY_THRESHOLD = .*|MALLEABILITY_THRESHOLD = $MALLEABILITY_THRESHOLD|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s/^ATTACHED = [0-9]/ATTACHED = $ATTACHED/g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^POLICY = .*|POLICY = $POLICY|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^HERCULES_PATH = .*|HERCULES_PATH = $HERCULES_PATH|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^HERCULES_CHECKPOINT_PATH = .*|HERCULES_CHECKPOINT_PATH = $HERCULES_CHECKPOINT_PATH|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^HERCULES_SNAPSHOT_PATH = .*|HERCULES_SNAPSHOT_PATH = $HERCULES_SNAPSHOT_PATH|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^DATA_HOSTFILE = .*|DATA_HOSTFILE = $DATA_HOSTFILE|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^ALLOC_DATA_HOSTFILE = .*|ALLOC_DATA_HOSTFILE = $ALLOC_DATA_HOSTFILE|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^METADATA_HOSTFILE = .*|METADATA_HOSTFILE = $METADATA_HOSTFILE|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^DEBUG_LEVEL = .*|DEBUG_LEVEL = $DEBUG_LEVEL|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^THREAD_POOL = .*|THREAD_POOL = $THREAD_POOL|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^STORAGE_SIZE = .*|STORAGE_SIZE = $STORAGE_SIZE|g" "$TEMPLATE_CONFIG_PATH"
						sed -i "s|^PREFETCH_SIZE = .*|PREFETCH_SIZE = $PREFETCH_SIZE|g" "$TEMPLATE_CONFIG_PATH"

						cat $TEMPLATE_CONFIG_PATH >"$CONFIG_PATH"
						cp $TEMPLATE_CONFIG_PATH."_TEMP" $TEMPLATE_CONFIG_PATH
						#				fi
						echo "${CONFIG_PATH}"
						#continue	## FIXED
						# exit 0
						set -x

						## The first job does not have dependencie (do not wait for another job to end).
						jid=$(sbatch ${SBATCH_FLAGS} --dependency=afterany:"${jid}" -N $NUMBER_OF_NODES $SCRIPT_NAME "$CONFIG_PATH" "$FILE_SIZE_PER_CLIENT"  "$TOTAL_NUMBER_OF_CLIENTS" "$CLIENTS_PER_NODE" "$IOR_FILE_PER_PROCESS" "$IOR_AVOID_CACHE" | cut -d ' ' -f4)
						echo $jid
						
						set +x

					done
				done
			done
		done
	done
done

set +o xtrace
