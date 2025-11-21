#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "crc.h"
#include "policies.h"

// Default session policy: ROUND ROBIN.
// int32_t session_plcy = ROUND_ROBIN_;
// Number of blocks conforming the handled dataset.
// int32_t n_blocks;
// Socket connecting the client to the imss server running in the same node.
int32_t matching_node_socket;

/********* LOCAL DATASET MANAGEMENT VARIABLES *********/

// Vector of characters specifying the position of each data element in a LOCAL dataset.
uint16_t *data_locations;
// Number of blocks written by the client in the current session.
uint64_t *num_blocks_written;
// Actual blocks written by the client.
uint32_t *blocks_written;

uint32_t get_policy_number(const char *policy_string)
{
	// // Invalid number of blocks to be sent.
	// if (dataset->num_data_elem <= 0)
	// 	return -1;

	// // Set blocks to be sent.
	// n_blocks = dataset->num_data_elem;
	int32_t session_policy = -1;

	// Set the corresponding policy.
	if (!strcmp(policy_string, "RR"))
	{
		session_policy = 0;
	}
	else if (!strcmp(policy_string, "BUCKETS"))
	{
		session_policy = 1;
	}
	else if (!strcmp(policy_string, "HASH"))
	{
		session_policy = 2;
	}
	else if (!strcmp(policy_string, "CRC16b"))
	{
		session_policy = 3;
	}
	else if (!strcmp(policy_string, "CRC64b"))
	{
		session_policy = 4;
	}
	else if (!strcmp(policy_string, "LOCAL"))
	{
		session_policy = 5;

		// Initialize variables hanlding LOCAL datasets.
		// data_locations = dataset->data_locations;
		// num_blocks_written = dataset->num_blocks_written;
		// blocks_written = dataset->blocks_written;
		// slog_debug("data_locations=%u, num_blocks_written=%u, blocks_written=%u", *data_locations, *num_blocks_written, *blocks_written);
	}
	else if (!strcmp(policy_string, "ZCOPY"))
	{
		session_policy = 6;

		// Initialize variables hanlding LOCAL datasets.
		// data_locations = dataset->data_locations;
		// num_blocks_written = dataset->num_blocks_written;
		// blocks_written = dataset->blocks_written;
		// slog_debug("data_locations=%u, num_blocks_written=%u, blocks_written=%u", *data_locations, *num_blocks_written, *blocks_written);
	}
	else
	{
		slog_error("HERCULES_ERR_SETPLCY_INVLD : %s", policy_string);
		perror("HERCULES_ERR_SETPLCY_INVLD");
		return -1;
	}

	slog_debug("policy_string=%s, session_policy=%d", policy_string, session_policy);

	return session_policy;
}

int32_t set_policy_dataset(dataset_info **dataset)
{
	// Save the connection to the imss server running in the same node.
	matching_node_socket = (*dataset)->local_conn;
	slog_info("matching_node_socket = %d", matching_node_socket);
	(*dataset)->session_plcy = get_policy_number((*dataset)->policy);
	if ((*dataset)->session_plcy < 0)
	{
		return -1;
	}
	return (*dataset)->session_plcy;
}

// int32_t set_policy_metadata(imss_info *hercules_info)
// {

// 	hercules_info->session_plcy = get_policy_number(dataset->policy);
// 	if (dataset->session_plcy < 0)
// 	{
// 		return -1;
// 	}
// 	return dataset->session_plcy;
// }

// int32_t get_policy()
// {
// 	return session_plcy;
// }

int32_t RoundRobin(int32_t n_servers, int32_t n_msg, const char *fname)
{
	int32_t next_server = -1;
	uint16_t crc_ = 0;
	crc_ = crc16(fname, strlen(fname));
	next_server = crc_ % n_servers;
	slog_debug("fname=%s, strlen(fname)=%d, next_server=%d, crc_=%d, n_servers=%d", fname, strlen(fname), next_server, crc_, n_servers);
	// fprintf(stderr, "fname=%s, strlen(fname)=%lu, next_server=%d, crc_=%d, n_servers=%d\n", fname, strlen(fname), next_server, crc_, n_servers);

	// slog_debug("fname=%s, new_dataset->original_name=%s, stat_dataset_res=%ld, next_server=%d, crc_=%d, n_servers=%d", fname, new_dataset->original_name, stat_dataset_res, next_server, crc_, n_servers);

	// Next server receiving the following block.
	next_server = (next_server + n_msg) % n_servers;
	return next_server;
}

// int32_t Buckets(int32_t n_servers, int32_t n_msg, char *fname)
// {
// 	int32_t next_server = -1;
// 	uint16_t crc_ = 0;

// 	crc_ = crc16(fname, strlen(fname));

// 	// First server that received a block from the current file.
// 	//  incluir "initial_server" en el dataset.
// 	uint32_t initial_server = crc_ % n_servers;

// 	if (n_blocks < n_servers)

// 		n_blocks = n_servers;

// 	// Number of servers that will be storing one additional block.
// 	uint32_t one_more_block = n_blocks % n_servers;

// 	// Number of blocks that these servers will be storing.
// 	uint32_t blocks_srv = (n_blocks / n_servers) + 1;

// 	// The block will be handled by those servers storing one more block.
// 	if (n_msg < (blocks_srv * one_more_block))
// 	{
// 		next_server = n_msg / blocks_srv;

// 		next_server = (next_server + initial_server) % n_servers;
// 	}
// 	// The block will be handled by those storing one block less.
// 	else
// 	{
// 		next_server = (n_msg - (blocks_srv * one_more_block)) / (blocks_srv - 1);

// 		next_server = (initial_server + one_more_block + next_server) % n_servers;
// 	}
// 	return next_server;
// }

int32_t Hashed(int32_t n_servers, int32_t n_msg, const char *fname)
{
	int32_t next_server = 0;
	// Key identifying the current to-be-sent file block.
	char key[strlen(fname) + 64];
	sprintf(key, "%s%c%d", fname, '$', n_msg);

	uint32_t b = 378551;
	uint32_t a = 63689;
	uint32_t hash = 0;
	uint32_t i = 0;
	uint32_t length = strlen(key);

	// Create the  hash through the messages's content.
	for (i = 0; i < length; ++i)
	{
		hash = hash * a + (key[i]);
		a = a * b;
	}

	next_server = hash % n_servers;

	return next_server;
}

int32_t CRC(int32_t n_servers, const char *fname, int32_t bytes_)
{
	int32_t next_server = -1;
	char key[strlen(fname) + 64];
	switch (bytes_)
	{
	case 16:
		next_server = crc16(key, strlen(key)) % n_servers;
		break;
	case 64:
		next_server = crc64(0, (unsigned char *)key, strlen(key)) % n_servers;
		break;
	}
	return next_server;
}

/** 
 * @deprecated
 */
void FindNameForPolicy(const char *fname, char *passed_name, char server_type)
{
	// Dataset metadata request.
	dataset_info *new_dataset;
	int32_t stat_dataset_res = -2;
	if (server_type == TYPE_DATA_SERVER)
	{
		stat_dataset_res = stat_dataset(fname, &new_dataset, 0);
	}
	char *tmp = NULL;
	if (stat_dataset_res == -2)
	{ // dataset does not exists, we will use the provided fname.
		tmp = (char *)fname;
	}
	else
	{ // original name is used in case the file was renamed.
		tmp = new_dataset->original_name;
	}
	snprintf(passed_name, PATH_MAX, "%s", tmp);
	// slog_debug("fnameadd=%p, passed_nameadd=%p", fname, passed_name);
	// slog_debug("passed_name=%s", passed_name);
	if (passed_name == NULL)
	{
		perror("HERCULES_ERR_FIND_SERVER_GETTING_FILE_NAME");
		slog_error("HERCULES_ERR_FIND_SERVER_GETTING_FILE_NAME");
	}
	// if (!strcmp(fname, "imss://test-dir.0-0/mdtest_tree.0.0/file.mdtest.0.0"))
	// {
	// 	fprintf(stderr, "passed name = %s, stat_dataset_res=%d\n", passed_name, stat_dataset_res);
	// }
}

/**
 * @brief Method retrieving the server that will receive the following message attending a policy.
 * @return next server number (positive integer, >= 0) to send the operation according to the policy, on error -1 is returned,
 */
int32_t find_server(
	int32_t n_servers,
	int32_t n_msg,
	const char *fname,
	int32_t op_type,
	char server_type,
	int32_t session_plcy)
{
	int32_t next_server = -1;
	// TODO: delete passed_name from all functions.
	// char passed_name[PATH_MAX] = {0};

	// if (!strcmp(fname, "imss://test-dir.0-0/mdtest_tree.0.0/file.mdtest.0.0"))
	// {
	// 	fprintf(stderr, "fname = %s, session_plcy=%d\n", passed_name, session_plcy);
	// }

	switch (session_plcy)
	{
	// Follow a round robin policy.
	case ROUND_ROBIN_:
	{
		// FindNameForPolicy(fname, passed_name, server_type);
		// if (passed_name != NULL)
		{
			// next_server = RoundRobin(n_servers, n_msg, passed_name);
			next_server = RoundRobin(n_servers, n_msg, fname);
		}
	}
	break;

	// Follow a bucketbnn distribution.
	// case BUCKETS_:
	// {
	// 	FindNameForPolicy(fname, passed_name, server_type);
	// 	if (passed_name != NULL)
	// 	{
	// 		next_server = Buckets(n_servers, n_msg, passed_name);
	// 	}
	// }
	// break;

	// Follow a hashed distribution.
	case HASHED_:
	{
		// FindNameForPolicy(fname, passed_name, server_type);
		// if (passed_name != NULL)
		// {
		// next_server = Hashed(n_servers, n_msg, passed_name);
		next_server = Hashed(n_servers, n_msg, fname);
		// }
	}
	break;

	// Following another hashed distribution using Redis's CRC16.
	case CRC16_:
	{
		// FindNameForPolicy(fname, passed_name, server_type);
		// if (passed_name != NULL)
		// {
		// 	next_server = CRC(n_servers, passed_name, 16);
		next_server = CRC(n_servers, fname, 16);
		// }
	}
	break;

	// Following another hashed distribution using Redis's CRC64.
	case CRC64_:
	{
		// FindNameForPolicy(fname, passed_name, server_type);
		// if (passed_name != NULL)
		// {
		// 	next_server = CRC(n_servers, passed_name, 64);
		next_server = CRC(n_servers, fname, 64);
		// }
	}
	break;

	// Follow a LOCAL distribution.
	case LOCAL_:
	// ZERO COPY follows LOCAL distribution but reducing memory copies.
	case ZCOPY_:
	{
		slog_debug("op_type=%d", op_type);
		// next_server = matching_node_socket;
		next_server = RoundRobin(n_servers, n_msg, fname);
		break;
		// Operate in relation to the type of operation.
		// FIXME: improve the following case!
		switch (op_type)
		{
		// Retrieve the socket connection number associated to the server storing the block.
		case GET:
		{
			slog_debug("GET, data_locations[%d] = %u", n_msg, data_locations[n_msg]);
			if (data_locations[n_msg])
				next_server = (int32_t)(data_locations[n_msg] - 1);

			break;
		}
		// Store the connection number associated to the server storing the data element.
		case SET:
		{
			slog_debug("SET, data_locations[%d] = %u", n_msg, data_locations[n_msg]);
			switch (data_locations[n_msg])
			{
			// If the block was not written yet, write it in the local IMSS server and save the storage event.
			case 0:
			{
				next_server = matching_node_socket;

				data_locations[n_msg] = (uint16_t)(next_server + 1);

				// Save the block number that the client has written.
				blocks_written[(*num_blocks_written)++] = n_msg;
				slog_debug("next_server=%d, data_locations[%d]=%u", next_server, n_msg, data_locations[n_msg]);

				break;
			}
			// If the block was already stored, retrieve the corresponding server storing it.
			default:
			{
				next_server = (int32_t)(data_locations[n_msg] - 1);

				break;
			}
			}

			break;
		}
		}

		break;
	}
	default:
		break;
	}
	// slog_debug("session_plcy=%ld, fname=%s, next_server=%d, n_servers=%d, passed_name=%s", session_plcy, fname, next_server, n_servers, passed_name);
	slog_debug("session_plcy=%ld, fname=%s, next_server=%d, n_servers=%d", session_plcy, fname, next_server, n_servers);
	// slog_debug("fnameadd=%p, passed_nameadd=%p", fname, passed_name);

	// "next_server" must be a value between 0 and n_servers-1.
	if (next_server < 0 || next_server >= n_servers)
	{
		char err_msg[MAX_ERR_MSG_LEN];
		sprintf(err_msg, "ERR_HERCULES_FIND_SERVER_WRONG_VALUE, next_server=%d, n_servers=%d, session_plcy=%d", next_server, n_servers, session_plcy);
		perror(err_msg);
		slog_error("ERR_HERCULES_FIND_SERVER_WRONG_VALUE, next_server=%d, n_servers=%d", next_server, n_servers);
		// return -1;
		exit(-1);
	}

	slog_debug("next_server=%ld", next_server);
	return next_server;
}
