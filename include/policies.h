#ifndef POLICIES_
#define POLICIES_

#define ROUND_ROBIN_		0
#define BUCKETS_		1
#define HASHED_			2
#define CRC16_			3
#define CRC64_			4
#define LOCAL_ 			5
#define ZCOPY_           6

#define GET			6
#define SET			7

#include "imss.h"

//Method specifying the policy.
uint32_t get_policy_number(const char *policy_string);
int32_t set_policy_dataset  (dataset_info * dataset);

/**
 * @brief Method retriving the policy number setted by the "set_policy" method.
 * @return policy number according to the distribution policy chosen by the user.
 */
// int32_t get_policy();

/**
 * @brief Method retrieving the server that will receive the following message attending a policy.
 * @return next server number (positive integer, >= 0) to send the operation according to the policy, on error -1 is returned, 
*/
int32_t find_server (int32_t n_servers, int32_t n_msg, const char * fname, int32_t op_type, char server_type, int32_t session_plcy);

int32_t RoundRobin(int32_t n_servers, int32_t n_msg,char *fname);
// int32_t Buckets(int32_t n_servers, int32_t n_msg,char *fname);
int32_t Hashed(int32_t n_servers, int32_t n_msg,char *fname);
int32_t CRC(int32_t n_servers, char *fname, int32_t bytes_);
void FindNameForPolicy(const char *fname, char *passed_name, char server_type);


#endif
