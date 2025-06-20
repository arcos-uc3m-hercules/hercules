#!/bin/bash

SCRIPT_NAME="mdtest_beegfs_slurm.sh"
TOTAL_NUMBER_OF_FILES=1000000

#NODES_FOR_CLIENTS_RANGE=( 1 2 4 8 16 )
NODES_FOR_CLIENTS_RANGE=( 16 )
CLIENTS_PER_NODE_RANGE=( 16 )
TEST_TYPE="strong"

MAX_ITERATIONS=1

jid=-1

for loop_number in $(seq 1 $MAX_ITERATIONS);
do
for NODES_FOR_CLIENTS in "${NODES_FOR_CLIENTS_RANGE[@]}"
do
	for PROCESS_PER_NODE in "${CLIENTS_PER_NODE_RANGE[@]}"
	do
			## Calculates the number of nodes to be allocated.
			NUMBER_OF_NODES=$((NODES_FOR_CLIENTS))
			## Calculates the total number of process to be launched.
			TOTAL_NUMBER_OF_CLIENTS=$((NODES_FOR_CLIENTS*PROCESS_PER_NODE))
			## Calculates the amount of data per client according 
			## to the type of test. 
			if [ "$TEST_TYPE" = "weak" ]; then
				NUMBER_OF_FILES_PER_PROCESS=${TOTAL_NUMBER_OF_FILES}
			## Strong by default.
			else
				NUMBER_OF_FILES_PER_PROCESS=$((${TOTAL_NUMBER_OF_FILES} / ${TOTAL_NUMBER_OF_CLIENTS}))
			fi

			set -x
			## The first job does not have dependencie (do not wait for another job to end). 
			# if [ "$jid" -eq 1 ]; then
			# 	jid=$(sbatch -N $NUMBER_OF_NODES $SCRIPT_NAME "$NUMBER_OF_FILES_PER_PROCESS" "$TOTAL_NUMBER_OF_CLIENTS" "$PROCESS_PER_NODE" | cut -d ' ' -f4)
			# 	echo $jid
			# ## The following jobs wait for the previous job to finish.
			# else
				jid=$(sbatch --dependency=afterany:"${jid}" -N $NUMBER_OF_NODES $SCRIPT_NAME "$NUMBER_OF_FILES_PER_PROCESS" "$TOTAL_NUMBER_OF_CLIENTS" "$PROCESS_PER_NODE" | cut -d ' ' -f4)
				echo $jid
			# fi
			set +x
	done
done	 
done


set +o xtrace
