#pragma once
#include <clickhouse/client.h>
#include "ch_client_options.hpp"
#include "DatabaseInterface.hpp"
#include "node.hpp"
#include <algorithm>
#include <cstdio>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

static std::string ch_col_to_string(const clickhouse::ColumnRef &col,
                                    size_t row) {
  using namespace clickhouse;
  switch (col->Type()->GetCode()) {
  case Type::String:
  case Type::FixedString:
    return std::string(col->As<ColumnString>()->At(row));
  case Type::UInt8:
    return std::to_string(col->As<ColumnUInt8>()->At(row));
  case Type::UInt16:
    return std::to_string(col->As<ColumnUInt16>()->At(row));
  case Type::UInt32:
    return std::to_string(col->As<ColumnUInt32>()->At(row));
  case Type::UInt64:
    return std::to_string(col->As<ColumnUInt64>()->At(row));
  case Type::Int8:
    return std::to_string(col->As<ColumnInt8>()->At(row));
  case Type::Int16:
    return std::to_string(col->As<ColumnInt16>()->At(row));
  case Type::Int32:
    return std::to_string(col->As<ColumnInt32>()->At(row));
  case Type::Int64:
    return std::to_string(col->As<ColumnInt64>()->At(row));
  case Type::Float32:
    return std::to_string(col->As<ColumnFloat32>()->At(row));
  case Type::Float64:
    return std::to_string(col->As<ColumnFloat64>()->At(row));
  case Type::Bool:
    return col->As<ColumnBool>()->At(row) ? "1" : "0";
  case Type::Date:
    return std::to_string(col->As<ColumnDate>()->At(row));
  case Type::Date32:
    return std::to_string(col->As<ColumnDate32>()->At(row));
  case Type::DateTime:
    return std::to_string(col->As<ColumnDateTime>()->At(row));
  case Type::DateTime64:
    return std::to_string(col->As<ColumnDateTime64>()->At(row));
  /* one ColumnDecimal backs every Decimal width; StringAt() renders the value
     with its scale so we never have to format an Int128 by hand */
  case Type::Decimal:
  case Type::Decimal32:
  case Type::Decimal64:
  case Type::Decimal128:
    return col->As<ColumnDecimal>()->StringAt(row);
  case Type::Enum8:
    return std::string(col->As<ColumnEnum8>()->NameAt(row));
  case Type::Enum16:
    return std::string(col->As<ColumnEnum16>()->NameAt(row));
  case Type::UUID: {
    /* not the canonical 8-4-4-4-12 spelling, but stable and comparable, which
       is all any caller needs it for */
    auto uuid = col->As<ColumnUUID>()->At(row);
    char buf[33];
    snprintf(buf, sizeof(buf), "%016llx%016llx",
             static_cast<unsigned long long>(uuid.first),
             static_cast<unsigned long long>(uuid.second));
    return std::string(buf);
  }
  case Type::JSON:
    return std::string(col->As<ColumnJSON>()->At(row));
  case Type::Nullable: {
    auto nullable = col->As<ColumnNullable>();
    if (nullable->IsNull(row))
      return "";
    return ch_col_to_string(nullable->Nested(), row);
  }
  default:
    /* Array/Tuple/Map/LowCardinality and friends land here. Returning "" made
       two different values compare equal, which silently passed the
       --compare-result-with-setting oracle; a visible marker at least shows up
       in the dumped result sets. */
    return "<unsupported:" + col->Type()->GetName() + ">";
  }
}

/* How many rows of a workload query's result set are kept in memory. A bulk
   SELECT over --records=12M rows returns millions of rows, and nothing in the
   workload path reads them: execute_sql() only asks for the row count to put in
   the thread log. Materialising them all cost ~90 bytes per row per thread
   (a std::vector<std::string> plus its heap buffer), so a handful of range
   scans across --threads=100 was enough to exhaust the box and get pstress
   OOM-killed. The rows are still received and decoded, so the server side of
   the query is exercised exactly as before, they are just no longer hoarded.

   The oracles that diff two result sets are the one caller that genuinely needs
   every row, so they lift the cap - see ch_result_row_limit(). */
static const size_t CH_KEEP_ROWS_DEFAULT = 1000;

/* Computed once: unlimited when a compare-result oracle is enabled, capped
   otherwise. */
static size_t ch_result_row_limit() {
  static const size_t limit =
      (options->at(Option::COMPARE_RESULT)->getBool() ||
       options->at(Option::COMPARE_RESULT_WITH_SETTING)->getBool())
          ? std::numeric_limits<size_t>::max()
          : CH_KEEP_ROWS_DEFAULT;
  return limit;
}

class ClickHouseDatabase : public DatabaseInterface {
private:
  std::unique_ptr<clickhouse::Client> client;
  query_result last_result;
  /* rows the server returned, which is not last_result.size() once the cap
     above kicks in */
  size_t last_row_count = 0;
  std::string last_error;
  int last_error_number = 0;
  /* A copy of what connect() was handed, so a connection that dies mid-run can
     be rebuilt from run_query() without going back out to Thd1. */
  std::unique_ptr<workerParams> conn_params;

  /* Connections this worker rebuilt after a socket-level failure; see
     idle_reconnects_count(). */
  size_t idle_reconnects = 0;

  /* Did the last run_query_once() fail because the connection itself broke,
     as opposed to the server answering with an error? */
  bool connection_broken = false;

  /* Run a query, keeping at most max_rows of its result set.

     The ClickHouse Cloud front end closes a native connection that has carried
     no bytes for about 45 seconds (TCP keepalive does not count), and it can
     hand a reconnecting client a flow whose server side never comes up; the
     client sees an SSL or socket error, or a server-side NetException about
     reading from its socket, and nothing reaches the server. Workers idle that
     long routinely: while two threads load --records, while a verifier holds
     every worker paused, while one waits on a table lock. So a query that
     failed at the connection level is run once more on a fresh connection, and
     the recovery is counted and reported in the thread log. Only a reconnect
     that fails outright stays fatal (CR_SERVER_LOST), which is still what a
     server that has gone away looks like. */
  bool run_query(const std::string &query, size_t max_rows) {
    const bool ok = run_query_once(query, max_rows);
    if (ok || !connection_broken)
      return ok;
    client.reset();
    if (!conn_params || !connect(*conn_params)) {
      last_error_number = CR_SERVER_LOST;
      return false;
    }
    idle_reconnects++;
    const bool retried = run_query_once(query, max_rows);
    if (!retried && connection_broken)
      last_error_number = CR_SERVER_LOST;
    return retried;
  }

  bool run_query_once(const std::string &query, size_t max_rows) {
    /* clear() keeps the outer vector's capacity, which after one 12M-row select
       is 288MB per thread that is never handed back. Drop the buffer outright
       once it has grown past anything worth reusing. */
    if (last_result.capacity() > 4 * CH_KEEP_ROWS_DEFAULT)
      query_result().swap(last_result);
    else
      last_result.clear();
    last_row_count = 0;
    last_error.clear();
    last_error_number = 0;
    connection_broken = false;
    /* Only reachable when the reconnect in run_query() also failed. */
    if (!client) {
      last_error = "not connected";
      last_error_number = CR_SERVER_LOST;
      return false;
    }
    try {
      client->Execute(
          clickhouse::Query(query).OnData([&](const clickhouse::Block &block) {
            const size_t rows = block.GetRowCount();
            const size_t cols = block.GetColumnCount();
            const size_t keep =
                std::min(rows, max_rows > last_result.size()
                                   ? max_rows - last_result.size()
                                   : size_t(0));
            for (size_t row = 0; row < keep; ++row) {
              std::vector<std::string> row_data;
              row_data.reserve(cols);
              for (size_t col = 0; col < cols; ++col)
                row_data.push_back(ch_col_to_string(block[col], row));
              last_result.push_back(std::move(row_data));
            }
            last_row_count += rows;
          }));
      return true;
    } catch (const clickhouse::ServerException &e) {
      /* The server answered, and the answer was an error. Nothing is wrong with
         the connection, so the worker keeps using it - unless the "error" is
         the server reporting that it could not read this very connection
         (a NetException such as "Timeout exceeded while reading from socket"),
         which the front end produces for a half-dead flow; the next statement
         on it fails at the SSL layer, so treat it as a broken connection now. */
      last_error = e.what();
      if (last_error.find("DB::NetException") != std::string::npos) {
        connection_broken = true;
        last_error_number = CR_SERVER_LOST;
      } else {
        last_error_number = 1;
      }
      return false;
    } catch (const std::exception &e) {
      /* Anything else came from the socket or the protocol decoder, both of
         which leave the stream mid-packet: every later query on this client
         would be reading the tail of this one. run_query() throws the
         connection away and retries on a fresh one. */
      last_error = e.what();
      last_error_number = CR_SERVER_LOST;
      connection_broken = true;
      return false;
    }
  }

public:
  ClickHouseDatabase() = default;

  bool connect(const workerParams &myParams) override {
    /* Outside the try so the catch below can name them in its hint. */
    const bool secure = options->at(Option::SECURE)->getBool();
    const int port =
        myParams.port > 0 ? myParams.port : ch_default_port(secure);
    try {
      clickhouse::ClientOptions opts;
      opts.SetHost(myParams.address)
          .SetPort(port)
          .SetUser(myParams.username)
          .SetPassword(myParams.password);
      ch_apply_client_options(
          opts, secure, options->at(Option::CH_SOCKET_TIMEOUT)->getInt());
      /* Connect without default database first to create it if needed */
      client = std::make_unique<clickhouse::Client>(opts);

      bool replicated = opt_string(PORT).find(',') != std::string::npos;
      std::string db = myParams.database;
      if (replicated) {
        client->Execute("CREATE DATABASE IF NOT EXISTS " + db +
                        " ENGINE = Replicated('/clickhouse/databases/" + db +
                        "', '{shard}', '{replica}')");
      } else {
        client->Execute("CREATE DATABASE IF NOT EXISTS " + db);
      }

      opts.SetDefaultDatabase(db);
      client = std::make_unique<clickhouse::Client>(opts);

      /* session settings from the settings file, e.g. the allow_experimental_*
         flags that gate experimental table settings. Applied to every
         connection, including reconnects, since tryreconnet() comes back
         through here. A bad name throws and is reported by the catch below —
         failing loudly beats creating tables without the feature. */
      for (const auto &setting : g_session_settings)
        client->Execute("SET " + setting);

      conn_params = std::make_unique<workerParams>(myParams);
      return true;
    } catch (const std::exception &e) {
      std::cerr << "ClickHouse connect error [" << myParams.address << ":"
                << port << "]: " << e.what()
                << ch_connect_hint(myParams.address, port, secure) << std::endl;
      return false;
    }
  }

  void disconnect() override { client.reset(); }

  /* The workload path: nothing reads the rows, so only keep a bounded sample. */
  bool execute_query(const std::string &query) override {
    return run_query(query, ch_result_row_limit());
  }

  /* Prepared statements are a MySQL C API feature; the native ClickHouse
     protocol has none, so the caller falls back to the plain SELECT. */
  bool execute_prepared_query(
      const std::string &, const std::vector<PreparedStatementParam> &,
      std::string *, unsigned long long *, bool *) override {
    return false;
  }

  query_result get_result() override { return last_result; }

  /* Callers here do read every row, but these are metadata queries against
     system tables and CHECK TABLE, whose results are small. */
  query_result get_query_result(const std::string &query) override {
    run_query(query, std::numeric_limits<size_t>::max());
    return last_result;
  }

  std::string get_single_value(const std::string &query) override {
    if (!run_query(query, 1))
      return "";
    if (!last_result.empty() && !last_result[0].empty())
      return last_result[0][0];
    return "";
  }

  /* the rows the server returned, not the rows kept */
  int get_affected_rows() override {
    return static_cast<int>(last_row_count);
  }

  std::string get_error() override { return last_error; }

  int get_error_number() override { return last_error_number; }

  size_t idle_reconnects_count() override { return idle_reconnects; }

  int get_server_version() override {
    std::string ver = get_single_value("SELECT version()");
    if (ver.empty())
      return 0;
    int major = 0, minor = 0, patch = 0;
    sscanf(ver.c_str(), "%d.%d.%d", &major, &minor, &patch);
    return major * 10000 + minor * 100 + patch;
  }

  ~ClickHouseDatabase() = default;
};
