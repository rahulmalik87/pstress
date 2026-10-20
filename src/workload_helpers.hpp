#ifndef __WORKLOAD_HELPERS_HPP__
#define __WORKLOAD_HELPERS_HPP__

/* Helpers shared between the workload generator files (random_test.cpp,
   random_values.cpp, column.cpp, table_create.cpp, table_alter.cpp,
   table_dml.cpp, ch_features.cpp, ch_table_settings.cpp) that are not part
   of the public interface in random_test.hpp. */

#include <atomic>
#include <random>
#include <string>
#include <vector>

struct Thd1;
struct Table;

/* Per-thread random engine, seeded by set_seed(). */
extern thread_local std::mt19937 rng;

/* Run-wide state owned by random_test.cpp. */
extern size_t number_of_records;        /* rows in the initial load per table */
extern std::vector<Table *> *all_tables; /* every table in the metadata */
extern std::vector<std::string> random_strs;
extern std::atomic<bool> run_query_failed;
extern int g_innodb_page_size;
extern bool encrypted_temp_tables;
extern bool encrypted_sys_tablelspaces;
extern std::vector<std::string> g_encryption;
extern std::vector<std::string> g_compression;
extern std::vector<std::string> g_row_format;
extern std::vector<std::string> g_tablespace;
extern std::vector<int> g_key_block_size;

/* random_test.cpp: random ", ALGORITHM=..., LOCK=..." clause for an ALTER;
   the picks are also returned through algo and lock when given */
std::string pick_algorithm_lock(std::string *const algo = nullptr,
                                std::string *const lock = nullptr);

/* random_values.cpp */
std::string rand_bit(int length);
std::string rand_date();
std::string rand_datetime();
std::string rand_timestamp(int precision = 0, bool include_timezone = true);

/* random_test.cpp: run CHECK TABLE and log a failure; false when it failed */
bool get_check_result(const std::string &sql, Thd1 *thd);

/* table_create.cpp: MySQL secondary engine sanity check at load time */
void validate_secondary_engine(Thd1 *thd);

#ifdef USE_CLICKHOUSE
/* ch_features.cpp */
std::string to_upper(const std::string &s);
/* "ReplicatedReplacingMergeTree(...)" -> "ReplacingMergeTree" */
std::string ch_engine_base(const std::string &engine);
/* the type ClickHouse actually stores for a column definition clause */
std::string ch_stored_type(const std::string &clause);
/* once per run before any table exists: drop views left by an earlier run */
void ch_prepare_materialized_views(Thd1 *thd);
#endif

#endif
