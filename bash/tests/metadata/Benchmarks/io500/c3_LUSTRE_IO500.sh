#!/bin/bash

SBATCH_FLAGS="--partition=large -A uc3m_a0-sciot" #--exclusive" 
SCRIPT_NAME="c3_io500_lustre_slurm.sh"
# Minimal test.
#IO500_CONFFILE="/home/tester004/gesanche/io500/Configurations/config-lustre-minimal.ini"
# Full test.
IO500_CONFFILE="/home/tester004/gesanche/hercules/bash/tests/metadata/Benchmarks/io500/Configurations/config-lustre-full.ini"


NODES_FOR_CLIENTS_RANGE=(10)
CLIENTS_PER_NODE_RANGE=(16)

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
	for NODES_FOR_CLIENTS in "${NODES_FOR_CLIENTS_RANGE[@]}"; do
		NUMBER_OF_NODES=${NODES_FOR_CLIENTS}
		for CLIENTS_PER_NODE in "${CLIENTS_PER_NODE_RANGE[@]}"; do
			TOTAL_NUMBER_OF_CLIENTS=$((NODES_FOR_CLIENTS * CLIENTS_PER_NODE))
			set -x
			## The first job does not have dependencie (do not wait for another job to end).
			jid=$(sbatch ${SBATCH_FLAGS} --dependency=afterany:"${jid}" -N $NUMBER_OF_NODES $SCRIPT_NAME ${TOTAL_NUMBER_OF_CLIENTS} ${CLIENTS_PER_NODE} "${IO500_CONFFILE}" | cut -d ' ' -f4)
			echo $jid
			set +x
		done
	done
done

set +o xtrace
