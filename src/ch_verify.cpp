#ifdef USE_CLICKHOUSE
/* clickhouse/client.h MUST come before any pstress headers (::Column collision) */
#include <clickhouse/client.h>
#include "ch_verify.hpp"
#include "random_test.hpp"
#include <chrono>
#include <iostream>
#include <thread>

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

void ch_verify_replicas(const std::string &addr, const std::vector<int> &ports,
                        const std::string &db, const std::string &user,
                        const std::string &pass,
                        const std::vector<std::string> &) {
  /* Connect to all replicas */
  std::vector<std::unique_ptr<clickhouse::Client>> clients;
  for (auto port : ports) {
    clickhouse::ClientOptions opts;
    opts.SetHost(addr).SetPort(port).SetUser(user).SetPassword(pass)
        .SetDefaultDatabase(db);
    clients.push_back(std::make_unique<clickhouse::Client>(opts));
  }

  /* Wait for replication queue to drain on replica[0] (max 60s) */
  std::cout << "\n==> Waiting for replication to catch up..." << std::endl;
  for (int i = 0; i < 60; i++) {
    std::string cnt = ch_query_single(
        *clients[0],
        "SELECT count() FROM system.replication_queue WHERE database='" + db + "'");
    if (cnt == "0") break;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  /* Get table list directly from ClickHouse — avoids relying on all_tables global */
  std::vector<std::string> table_names;
  clients[0]->Execute(
      clickhouse::Query("SELECT name FROM system.tables WHERE database='" + db +
                        "' ORDER BY name")
          .OnData([&](const clickhouse::Block &block) {
            for (size_t r = 0; r < block.GetRowCount(); ++r)
              table_names.push_back(
                  std::string(block[0]->As<clickhouse::ColumnString>()->At(r)));
          }));

  std::cout << "==> Verifying replica consistency for "
            << table_names.size() << " tables across "
            << ports.size() << " replicas..." << std::endl;

  bool all_ok = true;
  for (const auto &tname : table_names) {
    std::string cnt_sql  = "SELECT count() FROM " + tname;
    std::string csum_sql = "SELECT sum(cityHash64(*)) FROM " + tname +
                           " SETTINGS use_query_cache=0";

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

    std::cout << "  " << tname << ":";
    for (size_t i = 0; i < clients.size(); i++)
      std::cout << "  replica" << (i + 1) << "[cnt=" << counts[i]
                << " csum=" << checksums[i] << "]";
    std::cout << "  => " << (ok ? "OK" : "MISMATCH") << std::endl;
  }
  std::cout << "==> Replica verification: " << (all_ok ? "PASS" : "FAIL") << std::endl;
}
#endif
