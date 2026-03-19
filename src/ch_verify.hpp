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

/* Compare pstress in-memory metadata columns against actual ClickHouse schema.
   Connects to the first node and reports missing columns, extra columns, and
   nullability mismatches. */
void ch_verify_schema(const std::vector<std::string> &addrs,
                      const std::vector<int> &ports,
                      const std::string &db, const std::string &user,
                      const std::string &pass);
#endif
