#ifndef H_PERFORMANCE_RECORDS
#define H_PERFORMANCE_RECORDS

#include "slog.h"
#include <atomic>
#include <chrono>
#include <map>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <sys/time.h>
#include <time.h>
#include <vector>

struct arguments;

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

// Accumulates raw I/O bytes and maximum parallel client duration per server within the active time interval
// before flushing.
typedef struct
{
	int server_id = 0;				  // Latest storage server ID reported for this hostname.
	long long write_bytes = 0;			  // Total written bytes accumulated in the current interval.
	long long read_bytes = 0;			  // Total read bytes accumulated in the current interval.
	double max_write_time_sec = 0.0;		  // Maximum single-client cumulative write duration (seconds) in the current interval.
	double max_read_time_sec = 0.0;			  // Maximum single-client cumulative read duration (seconds) in the current interval.
	std::map<uint64_t, double> client_write_time_sec; // Per-client (worker_uid) cumulative active write duration in the current interval.
	std::map<uint64_t, double> client_read_time_sec;  // Per-client (worker_uid) cumulative active read duration in the current interval.
} PerformanceBucket;

/**
 * @brief Contains the minimal information needed to reply to a client
 * whose request was queued during a malleability operation.
 */
typedef struct
{
	ucp_worker_h ucp_worker; // Needed for send_data
	ucp_ep_h server_ep;	 // The client endpoint to reply to
	uint64_t worker_uid;	 // The UCP worker ID for the send operation
	char curr_req[PATH_MAX]; // The original request string, used for logging
} PendingRequestInfo;

// Malleability actions.
enum class scaling_action
{
	HOLD = 0,
	SCALE_UP = 1,
	SCALE_DOWN = -1
};

// Single scheduled event in an application profile for deterministic malleability.
typedef struct
{
	double trigger_sec = 0.0;
	int32_t target_servers = -1;
	bool executed = false;
} ApplicationProfileEvent;

// Use a map to store metrics, mapping server_id to its metrics.
// static std::map<int32_t, IOServerMetrics> backend_performance_metrics;
static std::map<std::string, IOServerMetrics> backend_performance_metrics;
// static std::vector<std::pair<int32_t, ElasticityMetric>> elasticy_records_history;
// static std::map<int32_t, std::vector<ElasticityMetric>> elasticity_records_history;
extern std::map<std::string, std::vector<ElasticityMetric>> elasticity_records_history;
// Maps each data server hostname to its accumulated raw byte totals for the current interval.
extern std::map<std::string, PerformanceBucket> pending_perf_bucket;
// Flags the next interval flush to discard in-flight metrics left over from a scaling transition.
extern std::atomic<bool> discard_transition_interval;
extern std::vector<ApplicationProfileEvent> application_profile_events;
extern std::atomic<bool> application_profile_active;
extern std::atomic<bool> application_profile_record_active;
extern std::atomic<bool> application_profile_t0_initialized;
extern std::chrono::steady_clock::time_point application_profile_t0;
static int best_number_of_servers = 0;
static std::atomic<int> number_of_history_records{0};
static unsigned int consecutive_commissioning_signals = 0;
static unsigned int consecutive_decommissioning_signals = 0;
static unsigned int consecutive_hold_signals = 0;

// static void PerformanceRecordsRemoveKey(int32_t server_id)
static int PerformanceRecordsRemoveKey(char *server_hostname)
{
	if (server_hostname == nullptr)
	{
		slog_error("HERCULES_ERR_PERFORMANCE_RECORDS_REMOVE_KEY: Invalid server hostname");
		return -1;
	}
	// backend_performance_metrics.erase(server_id);
	slog_debug("Removing server hostname %s", server_hostname);
	size_t elements_removed = backend_performance_metrics.erase(server_hostname);
	if (elements_removed > 0)
	{
		slog_debug("%s deleted.", server_hostname);
		return 0;
	}
	else
	{
		slog_debug("%s does not exist.", server_hostname);
		return -1;
	}
}

/**
 * @brief Loads and validates chronological malleability events from an application profile CSV file.
 *
 * @param path Path to the application profile file.
 * @param events Output vector cleared and populated with sorted ApplicationProfileEvent entries.
 * @return int 0 on success, -1 if the file could not be opened or path is invalid.
 */
int load_application_profile(const char *path, std::vector<ApplicationProfileEvent> &events);

/**
 * @brief Returns the elapsed time in seconds since application_profile_t0, or 0.0 if t0 is not initialized.
 *
 * @return double Elapsed seconds since the first I/O metric arrived.
 */
double get_application_profile_elapsed_sec();

/**
 * @brief Checks whether any scheduled application profile event is due at the current elapsed time.
 *
 * @return true if the profile is active, t0 is initialized, and at least one unexecuted event is due.
 */
bool has_due_application_profile_event();

/**
 * @brief Computes a deterministic 64-bit FNV-1a hash from the application and topology signature.
 *
 * @param cfg Configuration arguments containing application name, client count, and Hercules parameters.
 * @param out_signature Optional output buffer receiving the signature string.
 * @param sig_size Size of out_signature buffer in bytes.
 * @return uint64_t 64-bit FNV-1a hash of the signature.
 */
uint64_t compute_application_profile_hash(const struct arguments *cfg, char *out_signature, size_t sig_size);

/**
 * @brief Resolves cfg->application_profile_path from an explicit path or a hybrid hash-based filename.
 *
 * @param cfg Configuration arguments updated in place with the resolved profile path.
 * @param out_hash Optional output pointer receiving the computed 64-bit signature hash.
 * @return int 0 on success, -1 on invalid arguments.
 */
int resolve_application_profile_path(struct arguments *cfg, uint64_t *out_hash);

/**
 * @brief Creates or truncates the profile CSV and writes its self-describing header comments and column names.
 *
 * @param cfg Configuration arguments containing the resolved profile path and signature metadata.
 * @return int 0 on success, -1 on failure.
 */
int init_application_profile_recording(const struct arguments *cfg);

/**
 * @brief Appends a single scaling transition (elapsed_sec, active_servers) and flushes immediately to disk.
 *
 * @param path Target CSV file path.
 * @param elapsed_sec Elapsed seconds since application_profile_t0.
 * @param active_servers Number of active storage servers after the completed scaling step.
 * @return int 0 on success, -1 on failure.
 */
int record_application_profile_event(const char *path, double elapsed_sec, int32_t active_servers);

/**
 * @brief Discards transition-period performance buckets and resets elasticity history after a scaling operation.
 *
 * @param erased_host Optional decommissioned server hostname to remove from elasticity history.
 */
void clear_transition_performance_metrics(const std::string *erased_host);

/**
 * @brief Selects the data server ID and hostname to decommission for an application profile scale-down step.
 *
 * @param history_snapshot Current per-server elasticity history snapshot.
 * @param current_servers Number of currently active data servers.
 * @param out_key Output hostname of the chosen server when available.
 * @return int Target server ID to decommission, or -1 if no candidate exists.
 */
int select_profile_decommission_candidate(const std::map<std::string, std::vector<ElasticityMetric>> &history_snapshot, int32_t current_servers, std::string &out_key);

#endif // H_PERFORMANCE_RECORDS