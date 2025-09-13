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
    ServerMetrics write;
    ServerMetrics read;
} IOServerMetrics;

typedef struct
{
    int server_id = 0;
    double rio = 0.0; // recomended io throughput.
    double aio = 0.0; // acctual I/O throughput.
    double write_performance = 0.0;
    double read_performance = 0.0;
    double overall_performance = 0.0;
    // double tolerance_threshold = 0.0;
} ElasticityMetric;

// Use a map to store metrics, mapping server_id to its metrics.
static std::map<int32_t, IOServerMetrics> backend_performance_metrics;
// static std::vector<std::pair<int32_t, ElasticityMetric>> elasticy_records_history;
static std::map<int32_t, std::vector<ElasticityMetric>> elasticity_records_history;
static int best_number_of_servers = 0;
static int number_of_history_records = 0;

#endif // H_PERFORMANCE_RECORDS