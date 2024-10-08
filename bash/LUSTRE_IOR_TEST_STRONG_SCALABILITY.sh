#!/bin/bash

SCRIPT_NAME="ior_lustre_slurm.sh"
#FILE_SIZE=$((1024*1024*1))
FILE_SIZE=$((1024*1024*100))

# 0 = Shared single file, 1 = File per process.
IOR_FILE_PER_PROCESS=1
# 0 = Do not avoid cache, 1 = Avoid cache (recommend, https://ior.readthedocs.io/en/latest/userDoc/tutorial.html).
IOR_AVOID_CACHE=1

#NODES_FOR_CLIENTS_RANGE=( 1 2 4 8 16 32 )
NODES_FOR_CLIENTS_RANGE=( 16 )
CLIENTS_PER_NODE_RANGE=( 16 )
BLOCK_SIZE_RANGE=( 512 )
TEST_TYPE="strong"

MAX_ITERATIONS=1

jid=1

for loop_number in $(seq 1 $MAX_ITERATIONS);
do
for NODES_FOR_CLIENTS in "${NODES_FOR_CLIENTS_RANGE[@]}"
do
	for PROCESS_PER_NODE in "${CLIENTS_PER_NODE_RANGE[@]}"
	do
		for BLOCK_SIZE in "${BLOCK_SIZE_RANGE[@]}"
		do
			## Calculates the number of nodes to be allocated.
			NUMBER_OF_NODES=$((NODES_FOR_CLIENTS))
			## Calculates the total number of process to be launched.
			TOTAL_NUMBER_OF_CLIENTS=$((NODES_FOR_CLIENTS*PROCESS_PER_NODE))
			## Calculates the amount of data per client according 
			## to the type of test. 
			if [ "$TEST_TYPE" = "weak" ]; then
				FILE_SIZE_PER_CLIENT=$((1024*1024))
			## Strong by default.
			else
				FILE_SIZE_PER_CLIENT=$((FILE_SIZE/TOTAL_NUMBER_OF_CLIENTS))
			fi

			set -x
			## The first job does not have dependencie (do not wait for another job to end). 
			if [ "$jid" -eq 1 ]; then
				jid=$(sbatch -N $NUMBER_OF_NODES $SCRIPT_NAME "$FILE_SIZE_PER_CLIENT" "$TOTAL_NUMBER_OF_CLIENTS" "$PROCESS_PER_NODE" "$IOR_FILE_PER_PROCESS" "$IOR_AVOID_CACHE" | cut -d ' ' -f4)
				echo $jid
			## The following jobs wait for the previous job to finish.
			else
				jid=$(sbatch --dependency=afterany:"${jid}" -N $NUMBER_OF_NODES $SCRIPT_NAME "$FILE_SIZE_PER_CLIENT" "$TOTAL_NUMBER_OF_CLIENTS" "$PROCESS_PER_NODE" "$IOR_FILE_PER_PROCESS" "$IOR_AVOID_CACHE" | cut -d ' ' -f4)
				echo $jid
			fi
			set +x

		done
	done
done	 
done


set +o xtrace
