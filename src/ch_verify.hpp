#pragma once
#ifdef USE_CLICKHOUSE
#include <functional>
#include <shared_mutex>
#include <string>
#include <vector>

struct MVInfo;
struct Thd1;

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

/* Abort the run when --engine does not name an engine this ClickHouse has, so a
   typo fails once here with a clear message instead of failing every CREATE
   TABLE. Does nothing if system.table_engines cannot be read. */
void ch_validate_engine(const std::string &addr, int port,
                        const std::string &db, const std::string &user,
                        const std::string &pass);

/* Abort when --create-mv is asked for but the server has no
   materialized_views_populate_atomically: every CREATE MATERIALIZED VIEW would
   fail and the run would finish looking healthy while testing nothing. Does
   nothing when --create-mv is 0. */
void ch_validate_mv_support(const std::string &addr, int port,
                            const std::string &db, const std::string &user,
                            const std::string &pass);

/* Check one materialized view against its source table and print the verdict.

   A view mirrors its source, so once nothing is inserting it has to hold exactly
   the source's rows: that is the guarantee an atomic POPULATE makes about rows
   inserted while it ran. THE CALLER MUST HAVE QUIESCED THE WORKLOAD — comparing
   counts against a table still being inserted into is meaningless.

   query_one runs a query and returns the first column of its first row, so the
   same check serves a worker thread (through its own connection) and the
   end-of-run pass (through a clickhouse::Client).

   A non-empty mutation_reason suppresses the comparison and is printed as the
   cause: a source that was mutated, truncated, retyped or had a failed insert is
   legitimately out of sync with its views. A view created with
   materialized_views_populate_atomically=0 is expected to mismatch and is
   reported without failing the run. */
void ch_report_mv_check(
    const MVInfo &mv, const std::string &mutation_reason,
    const std::string &src_engine,
    const std::function<std::string(const std::string &)> &query_one,
    Thd1 *thd);

/* End-of-run pass over every materialized view still present, run after the
   worker threads have joined and the system is therefore already quiescent.
   Prints a per-view verdict plus the run totals, and sets run_query_failed on a
   real mismatch. */
void ch_verify_materialized_views(const std::vector<std::string> &addrs,
                                  const std::vector<int> &ports,
                                  const std::string &db,
                                  const std::string &user,
                                  const std::string &pass);

/* Compare pstress in-memory metadata columns against actual ClickHouse schema.
   Connects to the first node and reports missing columns, extra columns, and
   nullability mismatches. Returns true if all tables match, false otherwise. */
bool ch_verify_schema(const std::vector<std::string> &addrs,
                      const std::vector<int> &ports,
                      const std::string &db, const std::string &user,
                      const std::string &pass);
#endif
