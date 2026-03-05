#ifndef HERCULES_INMEMORY
#define HERCULES_INMEMORY

#include "comms.h"
#define KB 1024L
#define MB 1048576L
#define GB 1073741824UL

#include <string>
// #include <cstring>
#include "imss.h"
#include "arg_parser.h"
#include "cfg_parse.h"

static u_int16_t HERCULES_THREAD_POOL_SIZE = 1;
extern u_int16_t CONF_MALLEABILITY_STATUS;
extern u_int16_t ASYNC_IO;
// using std::string;

#ifdef __cplusplus
extern "C"
{
#endif

    /* Method initializing an instance of the HERCULES in-memory storage system.

        RECEIVES:	rank 		       - Integer identifying the current application process among the application itself.
                    backend_strg_size  - Storage size in KILOBYTES assigned to each Hercules instance.
                    server_port_num    - Port that a future IMSS deployment will be binding to.
                    deploy_metadata    - Flag determining if a metadata server shall be deployed in the current process.
                    metadata_port_num  - Port that the IMSS metadata server will be binding to.
                    metadata_buff_size - Storage size in KILOBYTES assigned to the IMSS metadata server.
                    metadata_file      - File that will be accessed in order to retrieve previous metadata structures (dataset and IMSS structures).

        RETURNS: 	 0 - Successful deployment.
                    -1 - In case of error.
    */
    int32_t hercules_init(uint32_t rank, uint64_t backend_strg_size, uint16_t server_port_num, int32_t deploy_metadata, uint16_t metadata_port_num, uint64_t metadata_buff_size, char *metadata_file);

    /* Method releasing an instance of the HERCULES in-memory storage system.

        RETURNS: 	 0 - Release operation performed successfully.
                    -1 - In case of error.
    */
    int32_t hercules_release();

    /**
     * @brief Get the Configuration of the Hercules deployment from enviroment
     * variables or a configuration file.
     *
     * @param args structure to store the configuration data.
     * @return 1 on success.
     */
    int getConfiguration(struct arguments *args);

    void getBlockInformation(std::string key, int *block_number, std::string *data_uri, std::string *file_name);

#ifdef __cplusplus
}
#endif

#endif
