#pragma once
#ifdef USE_CLICKHOUSE
#include <shared_mutex>
#include <string>
#include <vector>

/* Workers hold a shared_lock on this mutex for each query loop iteration.
   The verifier takes a unique_lock to pause all workers before checksumming. */
extern std::shared_mutex g_ch_verify_mutex;

/* Called at startup (before nodes start) to confirm replicas are in sync.
   addrs: one address per port, or a single address broadcast to all ports. */
void ch_verify_startup(const std::vector<std::string> &addrs,
                       const std::vector<int> &ports,
                       const std::string &db, const std::string &user,
                       const std::string &pass);

/* Verify row counts and checksums match across all replicas.
   Acquires unique_lock on g_ch_verify_mutex to pause workers while running.
   addrs: one address per port, or a single address broadcast to all ports. */
void ch_verify_replicas(const std::vector<std::string> &addrs,
                        const std::vector<int> &ports,
                        const std::string &db, const std::string &user,
                        const std::string &pass,
                        const std::vector<std::string> &table_names);

/* Drop settings the server does not know from the pool loaded out of the
   settings file, so a typo or a setting from another ClickHouse version does
   not fail every CREATE TABLE. A setting at probability 100 was asked for on
   every table, so an unknown name there aborts the run instead of quietly
   leaving the feature untested. Does nothing if the pool is empty or if
   system.merge_tree_settings cannot be read. */
void ch_validate_table_settings(const std::string &addr, int port,
                                const std::string &db, const std::string &user,
                                const std::string &pass);

/* Compare pstress in-memory metadata columns against actual ClickHouse schema.
   Connects to the first node and reports missing columns, extra columns, and
   nullability mismatches. Returns true if all tables match, false otherwise. */
bool ch_verify_schema(const std::vector<std::string> &addrs,
                      const std::vector<int> &ports,
                      const std::string &db, const std::string &user,
                      const std::string &pass);
#endif
