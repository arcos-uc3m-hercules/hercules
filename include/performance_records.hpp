#ifndef H_PERFORMANCE_RECORDS
#define H_PERFORMANCE_RECORDS

#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <map>
#include <vector>

// Structure to hold performance metrics
typedef struct
{
    // double total_req_time = 0.0;
    double total_data_time = 0.0;
    long long total_data_size = 0;
    long long num_operations = 0;
} ServerMetrics;

typedef struct
{
    int server_id = 0;
    ServerMetrics write;
    ServerMetrics read;
} IOServerMetrics;

typedef struct
{
    int server_id = 0;
    std::string server_hostname;
    double rio = 0.0; // recomended io throughput.
    double aio = 0.0; // acctual I/O throughput.
    double write_performance = 0.0;
    double read_performance = 0.0;
    double overall_performance = 0.0;
    // double tolerance_threshold = 0.0;
} ElasticityMetric;

/**
 * @brief Contains the minimal information needed to reply to a client
 * whose request was queued during a malleability operation.
 */
typedef struct
{
    ucp_worker_h ucp_worker; // Needed for send_data
    ucp_ep_h server_ep;      // The client endpoint to reply to
    uint64_t worker_uid;     // The UCP worker ID for the send operation
    char curr_req[PATH_MAX]; // The original request string, used for logging
} PendingRequestInfo;

// Malleability actions.
enum class scaling_action {
    HOLD = 0,
    SCALE_UP = 1,
    SCALE_DOWN = -1
};


// Use a map to store metrics, mapping server_id to its metrics.
// static std::map<int32_t, IOServerMetrics> backend_performance_metrics;
static std::map<std::string, IOServerMetrics> backend_performance_metrics;
// static std::vector<std::pair<int32_t, ElasticityMetric>> elasticy_records_history;
// static std::map<int32_t, std::vector<ElasticityMetric>> elasticity_records_history;
static std::map<std::string, std::vector<ElasticityMetric>> elasticity_records_history;
static int best_number_of_servers = 0;
static std::atomic<int> number_of_history_records{0};
static unsigned int consecutive_scale_up_signals = 0;
static unsigned int consecutive_scale_down_signals = 0;

// static void PerformanceRecordsRemoveKey(int32_t server_id)
static void PerformanceRecordsRemoveKey(char *server_hostname)
{
    // backend_performance_metrics.erase(server_id);
    backend_performance_metrics.erase(server_hostname);
}

#endif // H_PERFORMANCE_RECORDS