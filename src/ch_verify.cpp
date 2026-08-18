#ifdef USE_CLICKHOUSE
/* clickhouse/client.h MUST come before any pstress headers (::Column collision) */
#include <clickhouse/client.h>
#include "ch_client_options.hpp"
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
  /* The inner table of a materialized view is named .inner_id.<uuid>, which
     cannot be spelled in a FROM without quoting; count the view instead, which
     reads through to it. A view with TO likewise reads through to its target. */
  clients[0]->Execute(
      clickhouse::Query("SELECT name FROM system.tables WHERE database='" + db +
                        "' AND name NOT LIKE '.%' ORDER BY name")
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
    ch_apply_secure(opts, options->at(Option::SECURE)->getBool());
    clients.push_back(std::make_unique<clickhouse::Client>(opts));
  }
  return clients;
}

void ch_verify_startup(const std::vector<std::string> &addrs,
                       const std::vector<int> &ports,
                       const std::string &db, const std::string &user,
                       const std::string &pass) {
  std::cout << "\n==> [startup] Waiting for replication to catch up..." << std::endl;
  try {
    auto clients = make_clients(addrs, ports, db, user, pass);
    wait_for_replication(clients, db);
    if (!do_verify(clients, db)) {
      std::cerr << "ERROR: Replica mismatch at startup — aborting.\n";
      exit(EXIT_FAILURE);
    }
  } catch (const std::exception &e) {
    /* Otherwise this escapes to terminate() from inside the std::call_once in
       run_some_query() and prints a bare stack dump. */
    std::cerr << "ERROR: [startup] ClickHouse verification failed: " << e.what()
              << ch_connect_hint(addrs[0], ports.empty() ? 0 : ports[0],
                                 options->at(Option::SECURE)->getBool())
              << "\n";
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

/* ------------------------------------------------------------------------- */
/* Materialized view consistency                                             */
/* ------------------------------------------------------------------------- */

extern std::atomic<bool> run_query_failed;

/* Run totals, printed once at the end. The "off" counters are the negative
   control: views created with materialized_views_populate_atomically=0 have the
   old non-atomic population, so some of them are expected to come out
   inconsistent. If none ever does, the workload is not actually colliding with
   the population window and a green result on the atomic views means little. */
static std::atomic<long> g_mv_checked{0};
static std::atomic<long> g_mv_mismatch{0};
static std::atomic<long> g_mv_off_checked{0};
static std::atomic<long> g_mv_off_mismatch{0};
static std::atomic<long> g_mv_skipped{0};
static std::atomic<long> g_mv_inconclusive{0};

static std::string mv_col_list(const std::vector<std::string> &columns) {
  std::string list;
  for (const auto &col : columns) {
    if (!list.empty())
      list += ", ";
    list += col;
  }
  return list;
}

/* Up to 20 rows the view and the source disagree on, as one newline separated
   string so it fits through the single value executor. A positive count is a row
   the view holds more copies of than the source (duplicated by the population),
   a negative one a row the view is missing (lost by it). */
static std::string mv_diff_rows(const std::string &view, const std::string &src,
                                const std::string &cols,
                                const std::function<std::string(const std::string &)> &query_one) {
  const std::string row = "substring(toString(tuple(" + cols + ")), 1, 200)";
  return query_one(
      "SELECT arrayStringConcat(groupArray(line), '\n') FROM ("
      "  SELECT concat(if(delta > 0, 'view has ', 'view is missing '),"
      "                toString(abs(delta)), ' x ', r) AS line FROM ("
      "    SELECT r, sum(in_view) - sum(in_src) AS delta FROM ("
      "      SELECT " + row + " AS r, 1 AS in_view, 0 AS in_src FROM " + view +
      "      UNION ALL "
      "      SELECT " + row + " AS r, 0 AS in_view, 1 AS in_src FROM " + src +
      "    ) GROUP BY r HAVING delta != 0 ORDER BY r LIMIT 20"
      "  )"
      ") SETTINGS use_query_cache = 0");
}

void ch_report_mv_check(
    const MVInfo &mv, const std::string &mutation_reason,
    const std::string &src_engine,
    const std::function<std::string(const std::string &)> &query_one,
    Thd1 *thd) {
  /* reading the view reads through to its inner table; a TO view holds nothing
     itself, so its target is what has to be compared */
  const std::string view = mv.target.empty() ? mv.name : mv.target;
  const std::string cols = mv_col_list(mv.columns);
  const std::string label = mv.name + " (source " + mv.src_table + ", populate " +
                            (mv.populate_atomically ? "atomic" : "legacy") + ")";

  /* A mutated source is legitimately out of step with its views: mutations,
     truncates and dropped columns are never pushed to a view, so neither an
     exact comparison nor a weaker one holds. */
  if (!mutation_reason.empty()) {
    g_mv_skipped++;
    print_and_log("  MV " + label +
                      ": SKIPPED, a view cannot follow what happened to the "
                      "source: " + mutation_reason,
                  thd);
    return;
  }

  if (!mv.populate_atomically)
    g_mv_off_checked++;

  /* An engine that merges rows sharing a sorting key collapses away exactly the
     duplicates a non-atomic population would leave, so counts cannot be compared
     row for row. Every surviving source row must still be in the view, which
     catches lost rows but not duplicated ones.

     Both sides are serialised to a String first. Comparing the raw columns would
     drag in IN's NULL semantics (a row holding a NULL never matches, so it would
     read as lost) and would fail outright on a JSON column. */
  if (ch_engine_collapses_rows(src_engine)) {
    const std::string row = "toString(tuple(" + cols + "))";
    const std::string missing = query_one(
        "SELECT count() FROM (SELECT " + row + " AS r FROM " + mv.src_table +
        ") WHERE r NOT IN (SELECT " + row + " FROM " + view +
        ") SETTINGS use_query_cache = 0");
    if (missing.empty()) {
      g_mv_inconclusive++;
      print_and_log("  MV " + label + ": could not be checked, query failed",
                    thd);
      return;
    }
    if (missing == "0") {
      g_mv_checked++;
      print_and_log("  MV " + label + ": OK, no source row missing from the "
                    "view (" + src_engine + " collapses rows, so duplicates "
                    "cannot be checked)", thd);
      return;
    }
    g_mv_mismatch++;
    if (mv.populate_atomically)
      run_query_failed = true;
    else
      g_mv_off_mismatch++;
    print_and_log("  MV " + label + ": *** MISMATCH *** " + missing +
                  " source rows missing from the view", thd);
    return;
  }

  const std::string csum = "sum(cityHash64(toString(tuple(" + cols + "))))";
  const std::string cnt_src =
      query_one("SELECT count() FROM " + mv.src_table +
                " SETTINGS use_query_cache = 0");
  const std::string cnt_view =
      query_one("SELECT count() FROM " + view + " SETTINGS use_query_cache = 0");
  const std::string csum_src =
      query_one("SELECT " + csum + " FROM " + mv.src_table +
                " SETTINGS use_query_cache = 0");
  const std::string csum_view =
      query_one("SELECT " + csum + " FROM " + view +
                " SETTINGS use_query_cache = 0");

  if (cnt_src.empty() || cnt_view.empty() || csum_src.empty() ||
      csum_view.empty()) {
    g_mv_inconclusive++;
    print_and_log("  MV " + label + ": could not be checked, query failed", thd);
    return;
  }

  if (cnt_src == cnt_view && csum_src == csum_view) {
    g_mv_checked++;
    print_and_log("  MV " + label + ": OK, " + cnt_src + " rows, csum " +
                  csum_src, thd);
    return;
  }

  g_mv_mismatch++;
  const bool expected = !mv.populate_atomically;
  if (expected)
    g_mv_off_mismatch++;
  else
    run_query_failed = true;

  print_and_log("  MV " + label + ": " +
                (expected ? "mismatch as expected with the legacy "
                            "non-atomic population"
                          : "*** MISMATCH ***") +
                "  source[cnt=" + cnt_src + " csum=" + csum_src + "]  view[cnt=" +
                cnt_view + " csum=" + csum_view + "]",
                thd);
  const std::string diff = mv_diff_rows(view, mv.src_table, cols, query_one);
  if (!diff.empty())
    print_and_log("    rows the view and the source disagree on:\n" + diff, thd);
}

void ch_verify_materialized_views(const std::vector<std::string> &addrs,
                                  const std::vector<int> &ports,
                                  const std::string &db,
                                  const std::string &user,
                                  const std::string &pass) {
  std::vector<std::unique_ptr<clickhouse::Client>> clients;
  try {
    clients = make_clients(addrs, ports, db, user, pass);
  } catch (const std::exception &e) {
    std::cerr << "ERROR: materialized view verification cannot connect: "
              << e.what() << "\n";
    return;
  }
  if (ports.size() > 1)
    wait_for_replication(clients, db);

  clickhouse::Client &client = *clients[0];
  auto query_one = [&client](const std::string &sql) {
    try {
      return ch_query_single(client, sql);
    } catch (const std::exception &e) {
      return std::string();
    }
  };

  /* Views still present on the server, so a view dropped mid-run (already
     checked by the drop path) is not looked for here. */
  std::vector<std::string> names;
  try {
    client.Execute(clickhouse::Query("SELECT name FROM system.tables WHERE "
                                     "database = '" + db +
                                     "' AND engine = 'MaterializedView' ORDER "
                                     "BY name")
                       .OnData([&](const clickhouse::Block &block) {
                         for (size_t r = 0; r < block.GetRowCount(); ++r)
                           names.push_back(std::string(
                               block[0]->As<clickhouse::ColumnString>()->At(r)));
                       }));
  } catch (const std::exception &e) {
    std::cerr << "ERROR: cannot list materialized views: " << e.what() << "\n";
    return;
  }

  std::cout << "\n[" << now_str() << "] ==> Materialized view consistency ("
            << names.size() << " views)..." << std::endl;

  for (const auto &name : names) {
    MVInfo mv;
    bool known = false;
    {
      std::lock_guard<std::mutex> lk(g_materialized_views_mutex);
      for (const auto &registered : g_materialized_views) {
        if (registered.name == name) {
          mv = registered;
          known = true;
          break;
        }
      }
    }

    /* A view left behind by an earlier --step is not in this process's registry,
       and nothing here can tell whether its source was mutated in that earlier
       run or which populate setting it was created with. Any verdict would be a
       guess, so say what it is instead of inventing one. */
    if (!known) {
      std::cout << "  MV " << name
                << ": SKIPPED, not created by this run (left by an earlier "
                   "step, so its source history is unknown)\n";
      g_mv_skipped++;
      continue;
    }

    /* engine and mutation state come from the in-memory table */
    std::string mutation_reason;
    std::string src_engine;
    bool found_table = false;
    for (auto *table : *all_tables) {
      if (table->name_ == mv.src_table) {
        mutation_reason = table->mv_skip_reason();
        src_engine = table->engine;
        found_table = true;
        break;
      }
    }
    if (!found_table) {
      std::cout << "  MV " << name << ": SKIPPED, source table " << mv.src_table
                << " is gone\n";
      g_mv_skipped++;
      continue;
    }

    ch_report_mv_check(mv, mutation_reason, src_engine, query_one, nullptr);
  }

  /* A run where every view was skipped has verified nothing, and calling that
     PASS is how a broken experiment reads as a good result. */
  const long real_mismatch = g_mv_mismatch.load() - g_mv_off_mismatch.load();
  const char *verdict = real_mismatch > 0 ? "FAIL"
                        : g_mv_checked.load() > 0 ? "PASS"
                                                  : "NOTHING VERIFIED";
  std::cout << "[" << now_str() << "] ==> Materialized view consistency: "
            << verdict << "  (" << g_mv_checked.load() << " consistent, "
            << real_mismatch
            << " inconsistent with the atomic populate, "
            << g_mv_skipped.load() << " skipped, " << g_mv_inconclusive.load()
            << " not checked)" << std::endl;
  if (g_mv_checked.load() == 0 && g_mv_skipped.load() > 0)
    std::cout << "    NOTE: every view was skipped, so nothing was actually "
                 "compared. The skip reasons above say which part of the "
                 "workload to turn off: --no-update --no-delete, "
                 "--ch-alter-update=0 --ch-alter-delete=0, --truncate=0, "
                 "--recreate-table=0, --drop-column=0, --rename-column=0, "
                 "--modify-column=0, --kill-trx-prob-k=0"
              << std::endl;

  if (g_mv_off_checked.load() > 0) {
    std::cout << "    negative control: " << g_mv_off_mismatch.load() << " of "
              << g_mv_off_checked.load()
              << " views populated with materialized_views_populate_atomically=0"
                 " came out inconsistent"
              << std::endl;
    if (g_mv_off_mismatch.load() == 0)
      std::cout << "    NOTE: none of them did, so this workload is not "
                   "colliding with the population window — raise "
                   "--mv-snapshot-sleep-ms, the insert rate, or the thread "
                   "count before trusting a pass on the atomic views"
                << std::endl;
  }
}

/* Drop unknown settings from the pool read out of the settings file. Unknown at
   probability 100 is fatal: it was asked for on every table, so silently
   skipping it would make an experiment look like it ran when it never did. */
void ch_validate_table_settings(const std::string &addr, int port,
                                const std::string &db, const std::string &user,
                                const std::string &pass) {
  if (g_table_settings.empty())
    return;

  const bool secure = options->at(Option::SECURE)->getBool();
  std::set<std::string> known;
  try {
    clickhouse::ClientOptions opts;
    opts.SetHost(addr)
        .SetPort(port > 0 ? port : ch_default_port(secure))
        .SetUser(user)
        .SetPassword(pass)
        .SetDefaultDatabase(db);
    ch_apply_secure(opts, secure);
    clickhouse::Client client(opts);
    client.Execute(
        clickhouse::Query("SELECT name FROM system.merge_tree_settings")
            .OnData([&](const clickhouse::Block &block) {
              for (size_t r = 0; r < block.GetRowCount(); ++r)
                known.insert(
                    std::string(block[0]->As<clickhouse::ColumnString>()->At(r)));
            }));
  } catch (const std::exception &e) {
    print_and_log("Cannot read system.merge_tree_settings (" +
                      std::string(e.what()) +
                      "), using the table settings pool unchecked",
                  nullptr);
    return;
  }

  if (known.empty())
    return;

  std::vector<SettingSpec> kept;
  for (const auto &spec : g_table_settings) {
    if (known.count(spec.name) > 0) {
      kept.push_back(spec);
      continue;
    }
    if (spec.prob == 100) {
      print_and_log("Table setting " + spec.name +
                        " was requested for every table but this ClickHouse "
                        "does not have it — check the name in " +
                        options->at(Option::CH_TABLE_SETTINGS_FILE)->getString(),
                    nullptr);
      exit(EXIT_FAILURE);
    }
    print_and_log("Skipping unknown table setting " + spec.name +
                      ", this ClickHouse does not have it",
                  nullptr);
  }
  g_table_settings = std::move(kept);
}

void ch_validate_engine(const std::string &addr, int port,
                        const std::string &db, const std::string &user,
                        const std::string &pass) {
  const std::string requested = options->at(Option::ENGINE)->getString();
  if (requested.empty())
    return;

  /* system.table_engines lists bare names, so compare without arguments */
  std::string name = ch_resolve_engine(requested);
  auto args = name.find('(');
  if (args != std::string::npos)
    name.erase(args);

  const bool secure = options->at(Option::SECURE)->getBool();
  std::set<std::string> known;
  try {
    clickhouse::ClientOptions opts;
    opts.SetHost(addr)
        .SetPort(port > 0 ? port : ch_default_port(secure))
        .SetUser(user)
        .SetPassword(pass)
        .SetDefaultDatabase(db);
    ch_apply_secure(opts, secure);
    clickhouse::Client client(opts);
    client.Execute(
        clickhouse::Query("SELECT name FROM system.table_engines")
            .OnData([&](const clickhouse::Block &block) {
              for (size_t r = 0; r < block.GetRowCount(); ++r)
                known.insert(
                    std::string(block[0]->As<clickhouse::ColumnString>()->At(r)));
            }));
  } catch (const std::exception &e) {
    print_and_log("Cannot read system.table_engines (" + std::string(e.what()) +
                      "), using --engine=" + requested + " unchecked",
                  nullptr);
    return;
  }

  if (known.empty() || known.count(name) > 0)
    return;

  print_and_log("--engine=" + requested + " resolves to " + name +
                    ", which this ClickHouse does not have. Use a MergeTree "
                    "family engine, for example MergeTree (never collapses "
                    "rows, needed for the exact materialized view check) or "
                    "ReplacingMergeTree.",
                nullptr);
  exit(EXIT_FAILURE);
}

void ch_validate_mv_support(const std::string &addr, int port,
                           const std::string &db, const std::string &user,
                           const std::string &pass) {
  if (options->at(Option::CH_CREATE_MV)->getInt() == 0)
    return;

  /* Without these two the CREATE MATERIALIZED VIEW would fail on every attempt
     and the run would finish looking healthy while having tested nothing. */
  std::vector<std::string> needed{"materialized_views_populate_atomically"};
  if (options->at(Option::CH_MV_SNAPSHOT_SLEEP_MS)->getInt() > 0)
    needed.push_back("merge_tree_storage_snapshot_sleep_ms");

  const bool secure = options->at(Option::SECURE)->getBool();
  std::set<std::string> known;
  try {
    clickhouse::ClientOptions opts;
    opts.SetHost(addr)
        .SetPort(port > 0 ? port : ch_default_port(secure))
        .SetUser(user)
        .SetPassword(pass)
        .SetDefaultDatabase(db);
    ch_apply_secure(opts, secure);
    clickhouse::Client client(opts);
    client.Execute(
        clickhouse::Query("SELECT name FROM system.settings")
            .OnData([&](const clickhouse::Block &block) {
              for (size_t r = 0; r < block.GetRowCount(); ++r)
                known.insert(
                    std::string(block[0]->As<clickhouse::ColumnString>()->At(r)));
            }));
  } catch (const std::exception &e) {
    print_and_log("Cannot read system.settings (" + std::string(e.what()) +
                      "), running --create-mv unchecked",
                  nullptr);
    return;
  }
  if (known.empty())
    return;

  for (const auto &name : needed) {
    if (known.count(name) > 0)
      continue;
    print_and_log(
        "--create-mv needs the setting " + name +
            ", which this ClickHouse does not have. Every CREATE MATERIALIZED "
            "VIEW would fail and the run would test nothing. Use a server "
            "built from a master that contains the atomic POPULATE work "
            "(ClickHouse PR 108715)" +
            (name == "merge_tree_storage_snapshot_sleep_ms"
                 ? ", or pass --mv-snapshot-sleep-ms=0 to drop this one (which "
                   "makes the race far less likely to be hit)."
                 : "."),
        nullptr);
    exit(EXIT_FAILURE);
  }
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
  ch_apply_secure(opts, options->at(Option::SECURE)->getBool());
  std::unique_ptr<clickhouse::Client> client_ptr;
  try {
    client_ptr = std::make_unique<clickhouse::Client>(opts);
  } catch (const std::exception &e) {
    std::cerr << "ERROR: cannot connect to " << host << ":" << port << " — "
              << e.what()
              << ch_connect_hint(host, port,
                                 options->at(Option::SECURE)->getBool())
              << "\n";
    return false;
  }
  clickhouse::Client &client = *client_ptr;

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

    /* Check for extra columns in ClickHouse not present in metadata.
       _pstress_ver is a synthetic version column injected at CREATE TABLE
       time and is never stored in metadata — always skip it. */
    std::set<std::string> meta_names;
    for (auto *col : meta_cols)
      meta_names.insert(col->name_);
    for (const auto &cn : ch_col_order) {
      if (cn == "_pstress_ver")
        continue;
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
