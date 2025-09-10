#!/bin/bash

SCRIPT_NAME="ior_collectiveFile_lustre_slurm.sh"
FILE_SIZE=$((1024*1024*10))
#FILE_SIZE=$((1024*1024*100))

#NODES_FOR_CLIENTS_RANGE=( 16 )
NODES_FOR_CLIENTS_RANGE=( 1 4 8 16 )
#CLIENTS_PER_NODE_RANGE=( 1 2 4 8 16 32 )
CLIENTS_PER_NODE_RANGE=( 16 )
TEST_TYPE="weak"

jid=1

for TEST_NUMBER in {1..1}
do
	echo "Test number $TEST_NUMBER"
	for NODES_FOR_CLIENTS in "${NODES_FOR_CLIENTS_RANGE[@]}"
	do
		for PROCESS_PER_NODE in "${CLIENTS_PER_NODE_RANGE[@]}"
		do
			## Calculates the number of nodes to be allocated.
			NUMBER_OF_NODES=$((NODES_FOR_CLIENTS))		

			# Calculates the total number of process to be deployed.
			TOTAL_NUMBER_OF_PROCESSES=$((NODES_FOR_CLIENTS*PROCESS_PER_NODE))
			## Calculates the amount of data per client according 
			## to the type of test. 
			if [ "$TEST_TYPE" = "weak" ]; then
				FILE_SIZE_PER_CLIENT=$((1024*100))
			## Strong by default.
			else
				FILE_SIZE_PER_CLIENT=$((FILE_SIZE/TOTAL_NUMBER_OF_PROCESSES))
			fi
			
			set -x
			## The first job does not have dependencie (do not wait for another job to end). 
			if [ "$jid" -eq 1 ]; then
				jid=$(sbatch -N $NUMBER_OF_NODES $SCRIPT_NAME "$FILE_SIZE_PER_CLIENT" "$TOTAL_NUMBER_OF_PROCESSES" "$PROCESS_PER_NODE" | cut -d ' ' -f4)
				echo $jid
			## The following jobs wait for the previous job to finish.
			else
				jid=$(sbatch --dependency=afterany:"${jid}" -N $NUMBER_OF_NODES $SCRIPT_NAME "$FILE_SIZE_PER_CLIENT" "$TOTAL_NUMBER_OF_PROCESSES" "$PROCESS_PER_NODE" | cut -d ' ' -f4)
				echo $jid
			fi
			set +x

	    done	 
	done
done

set +o xtrace
