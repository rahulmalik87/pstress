#pragma once
#ifdef USE_CLICKHOUSE
#include <string>
#include <vector>

/* Verify row counts and checksums match across all replicas.
   Called after a multi-port (replicated) pstress run. */
void ch_verify_replicas(const std::string &addr, const std::vector<int> &ports,
                        const std::string &db, const std::string &user,
                        const std::string &pass,
                        const std::vector<std::string> &table_names);
#endif
