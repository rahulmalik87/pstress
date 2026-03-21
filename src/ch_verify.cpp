#ifdef USE_CLICKHOUSE
/* clickhouse/client.h MUST come before any pstress headers (::Column collision) */
#include <clickhouse/client.h>
#include "ch_verify.hpp"
#include "random_test.hpp"
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <thread>

extern std::vector<Table *> *all_tables;

std::shared_mutex g_ch_verify_mutex;

static std::string ch_col_value(const clickhouse::ColumnRef &col, size_t row) {
  using namespace clickhouse;
  switch (col->Type()->GetCode()) {
  case Type::UInt64:
    return std::to_string(col->As<ColumnUInt64>()->At(row));
  case Type::String:
    return std::string(col->As<ColumnString>()->At(row));
  case Type::Nullable: {
    auto n = col->As<ColumnNullable>();
    if (n->IsNull(row)) return "NULL";
    return ch_col_value(n->Nested(), row);
  }
  default:
    return "";
  }
}

static std::string ch_query_single(clickhouse::Client &c, const std::string &sql) {
  std::string result;
  c.Execute(clickhouse::Query(sql).OnData([&](const clickhouse::Block &block) {
    if (block.GetRowCount() > 0 && block.GetColumnCount() > 0)
      result = ch_col_value(block[0], 0);
  }));
  return result;
}

/* Wait for replication queues to drain on ALL replicas (max 60s). */
static void wait_for_replication(std::vector<std::unique_ptr<clickhouse::Client>> &clients,
                                 const std::string &db) {
  for (int i = 0; i < 60; i++) {
    bool all_drained = true;
    for (auto &c : clients) {
      std::string cnt = ch_query_single(
          *c, "SELECT count() FROM system.replication_queue WHERE database='" + db + "'");
      if (cnt != "0") { all_drained = false; break; }
    }
    if (all_drained) break;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

static std::string now_str() {
  auto t = std::time(nullptr);
  std::tm tm{};
  localtime_r(&t, &tm);
  std::ostringstream oss;
  oss << std::put_time(&tm, "%H:%M:%S");
  return oss.str();
}

/* Core verification: compare count+checksum on all replicas for every table. */
static bool do_verify(std::vector<std::unique_ptr<clickhouse::Client>> &clients,
                      const std::string &db) {
  std::vector<std::string> table_names;
  clients[0]->Execute(
      clickhouse::Query("SELECT name FROM system.tables WHERE database='" + db +
                        "' ORDER BY name")
          .OnData([&](const clickhouse::Block &block) {
            for (size_t r = 0; r < block.GetRowCount(); ++r)
              table_names.push_back(
                  std::string(block[0]->As<clickhouse::ColumnString>()->At(r)));
          }));

  std::cout << "\n[" << now_str() << "] ==> Verifying replica consistency for "
            << table_names.size() << " tables across "
            << clients.size() << " replicas..." << std::endl;

  bool all_ok = true;
  for (const auto &tname : table_names) {
    std::string cnt_sql  = "SELECT count() FROM " + tname;
    /* toString(tuple(*)) serialises every column including NULLs to a String,
       so cityHash64(String) always returns UInt64 (never Nullable). */
    std::string csum_sql = "SELECT sum(cityHash64(toString(tuple(*)))) FROM " +
                           tname + " SETTINGS use_query_cache=0";

    std::vector<std::string> counts, checksums;
    bool ok = true;
    for (auto &c : clients) {
      counts.push_back(ch_query_single(*c, cnt_sql));
      checksums.push_back(ch_query_single(*c, csum_sql));
    }
    for (size_t i = 1; i < clients.size(); i++) {
      if (counts[i] != counts[0] || checksums[i] != checksums[0])
        ok = false;
    }
    if (!ok) all_ok = false;

    std::cout << "  " << std::left << std::setw(20) << tname;
    for (size_t i = 0; i < clients.size(); i++)
      std::cout << "  r" << (i + 1) << "[cnt=" << std::setw(8) << counts[i]
                << " csum=" << checksums[i] << "]";
    std::cout << "  => " << (ok ? "OK" : "*** MISMATCH ***") << std::endl;
  }
  std::cout << "[" << now_str() << "] ==> Replica verification: "
            << (all_ok ? "PASS" : "FAIL") << std::endl;
  return all_ok;
}

/* addrs can be a single address (broadcast to all ports) or one per port. */
static std::vector<std::unique_ptr<clickhouse::Client>>
make_clients(const std::vector<std::string> &addrs,
             const std::vector<int> &ports,
             const std::string &db, const std::string &user,
             const std::string &pass) {
  std::vector<std::unique_ptr<clickhouse::Client>> clients;
  for (size_t i = 0; i < ports.size(); i++) {
    const std::string &host = (addrs.size() == 1) ? addrs[0] : addrs[i];
    clickhouse::ClientOptions opts;
    opts.SetHost(host).SetPort(ports[i]).SetUser(user).SetPassword(pass)
        .SetDefaultDatabase(db);
    clients.push_back(std::make_unique<clickhouse::Client>(opts));
  }
  return clients;
}

void ch_verify_startup(const std::vector<std::string> &addrs,
                       const std::vector<int> &ports,
                       const std::string &db, const std::string &user,
                       const std::string &pass) {
  std::cout << "\n==> [startup] Waiting for replication to catch up..." << std::endl;
  auto clients = make_clients(addrs, ports, db, user, pass);
  wait_for_replication(clients, db);
  if (!do_verify(clients, db)) {
    std::cerr << "ERROR: Replica mismatch at startup — aborting.\n";
    exit(EXIT_FAILURE);
  }
}

void ch_verify_replicas(const std::vector<std::string> &addrs,
                        const std::vector<int> &ports,
                        const std::string &db, const std::string &user,
                        const std::string &pass,
                        const std::vector<std::string> &) {
  /* Grab exclusive lock — pauses all worker threads at their next iteration
     boundary. Workers hold shared_lock per iteration via g_ch_verify_mutex. */
  std::unique_lock<std::shared_mutex> pause_lk(g_ch_verify_mutex);

  auto clients = make_clients(addrs, ports, db, user, pass);
  std::cout << "\n==> Waiting for replication to catch up..." << std::endl;
  wait_for_replication(clients, db);
  do_verify(clients, db);
}

/* Compare pstress in-memory metadata columns against actual ClickHouse schema.
   Uses the first node. Reports missing/extra columns and nullability mismatches.
   Returns true if all tables match, false on any mismatch. */
bool ch_verify_schema(const std::vector<std::string> &addrs,
                      const std::vector<int> &ports,
                      const std::string &db, const std::string &user,
                      const std::string &pass) {
  const std::string &host = addrs[0];
  int port = ports[0];

  clickhouse::ClientOptions opts;
  opts.SetHost(host).SetPort(port).SetUser(user).SetPassword(pass)
      .SetDefaultDatabase(db);
  clickhouse::Client client(opts);

  std::cout << "\n[" << now_str()
            << "] ==> Schema verification: metadata vs ClickHouse ("
            << host << ":" << port << ")..." << std::endl;

  bool all_ok = true;
  std::set<std::string> seen_tables;

  for (auto *table : *all_tables) {
    if (!seen_tables.insert(table->name_).second)
      continue; /* deduplicate: two nodes share all_tables */

    /* Fetch columns from ClickHouse ordered by position */
    std::map<std::string, std::string> ch_col_type; /* name -> CH type string */
    std::vector<std::string> ch_col_order;
    try {
      client.Execute(
          clickhouse::Query(
              "SELECT name, type FROM system.columns "
              "WHERE database='" + db + "' AND table='" + table->name_ + "' "
              "ORDER BY position")
              .OnData([&](const clickhouse::Block &block) {
                for (size_t r = 0; r < block.GetRowCount(); ++r) {
                  std::string n(block[0]->As<clickhouse::ColumnString>()->At(r));
                  std::string t(block[1]->As<clickhouse::ColumnString>()->At(r));
                  ch_col_type[n] = t;
                  ch_col_order.push_back(n);
                }
              }));
    } catch (const std::exception &e) {
      std::cout << "  " << table->name_ << ": query failed: " << e.what() << "\n";
      all_ok = false;
      continue;
    }

    const auto &meta_cols = *table->columns_;
    bool table_ok = true;

    /* Check every metadata column exists in ClickHouse with matching nullability */
    for (auto *col : meta_cols) {
      auto it = ch_col_type.find(col->name_);
      if (it == ch_col_type.end()) {
        std::cout << "  " << table->name_ << "." << col->name_
                  << ": MISSING in ClickHouse\n";
        table_ok = all_ok = false;
        continue;
      }
      const std::string &ch_type = it->second;
      bool ch_nullable = ch_type.rfind("Nullable(", 0) == 0;
      bool meta_nullable =
          col->null_val && options->at(Option::NULL_PROB)->getInt() > 0;
      if (ch_nullable != meta_nullable) {
        std::cout << "  " << table->name_ << "." << col->name_
                  << ": nullable MISMATCH  meta="
                  << (meta_nullable ? "Nullable" : "NOT NULL")
                  << "  ch=" << ch_type << "\n";
        table_ok = all_ok = false;
      }
    }

    /* Check for extra columns in ClickHouse not present in metadata */
    std::set<std::string> meta_names;
    for (auto *col : meta_cols)
      meta_names.insert(col->name_);
    for (const auto &cn : ch_col_order) {
      if (meta_names.find(cn) == meta_names.end()) {
        std::cout << "  " << table->name_ << "." << cn
                  << ": extra column in ClickHouse (not in metadata)\n";
        table_ok = all_ok = false;
      }
    }

    if (table_ok) {
      std::cout << "  " << std::left << std::setw(20) << table->name_
                << "  OK (" << meta_cols.size() << " columns)\n";
    }
  }

  std::cout << "[" << now_str() << "] ==> Schema verification: "
            << (all_ok ? "PASS" : "FAIL") << std::endl;
  return all_ok;
}
#endif
