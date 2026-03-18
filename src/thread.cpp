/* we include MySQLDatabase only if fork is mysql */
#ifdef USE_MYSQL
#include "MySQLDatabase.hpp"
#elif USE_DUCKDB
#include "DuckdbDatabase.hpp"
#elif USE_CLICKHOUSE
#include "ClickhouseDatabase.hpp"
#endif
#include "common.hpp"
#include "node.hpp"
#include "random_test.hpp"
#include "ring_buffer.hpp"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <random>
#include <sstream>
    std::atomic<bool>
        metadata_loaded(false);

#ifdef USE_MYSQL
std::mutex MySQLDatabase::conn_mutex;
#endif

void Node::workerThread(int number) {

  std::ofstream thread_log;
  std::ofstream client_log;
  std::ostringstream file_name;
  file_name << myParams.myName << "_"
            << options->at(Option::DATABASE)->getString() << "_thread_"
            << number << "_step_" << options->at(Option::STEP)->getInt();

  if (options->at(Option::LOG_CLIENT_OUTPUT)->getBool()) {
    setupClientOutputLog(client_log, myParams.logdir, file_name.str() + ".out");
  }
  setupClientOutputLog(thread_log, myParams.logdir, file_name.str() + ".log");

  if (options->at(Option::LOG_QUERY_DURATION)->getBool()) {
    thread_log.precision(3);
    thread_log << std::fixed;
    std::cerr.precision(3);
    std::cerr << std::fixed;
    std::cout.precision(3);
    std::cout << std::fixed;
  }

#ifdef USE_MYSQL
  std::unique_ptr<DatabaseInterface> db = std::make_unique<MySQLDatabase>();
#elif USE_DUCKDB
  std::unique_ptr<DatabaseInterface> db = std::make_unique<DuckDBDatabase>();
#elif USE_CLICKHOUSE
  std::unique_ptr<DatabaseInterface> db = std::make_unique<ClickHouseDatabase>();
#endif
  if (!db->connect(myParams)) {
    std::cout << "Failed to connect database " << std::endl;
    exit(EXIT_FAILURE);
  };

  Thd1 *thd =
      new Thd1(number, thread_log, general_log, client_log, std::move(db),
               performed_queries_total, failed_queries_total,
               options->at(Option::N_LAST_QUERIES)->getInt());

  thd->myParam = &myParams;

  /* load metadata by the last thread, this helps to ensure all thread are
   * initiated and can connect to db */
  if (number == options->at(Option::THREADS)->getInt() - 1) {
    if (!thd->load_metadata()) {
      std::cerr << "FAILED to load metadata " << std::endl;
      exit(EXIT_FAILURE);
    }
    metadata_loaded.store(true, std::memory_order_seq_cst);
  }

  /* wait untill metadata is finished */
  while (!metadata_loaded.load(std::memory_order_seq_cst)) {
    std::chrono::seconds dura(3);
    std::this_thread::sleep_for(dura);
    thread_log << "waiting for metadata load to finish" << std::endl;
  }

  auto result = thd->run_some_query();

  if (!result) {
    std::ostringstream errmsg;
    errmsg << "Thread with id " << thd->thread_id << " failed, check logs  in "
           << myParams.logdir << "/*log";
    std::cerr << errmsg.str() << std::endl;
    exit(EXIT_FAILURE);
  }
  if (thd->query_buffer.size() > 0) {
    thread_log << "last N SQL executed by thread " << std::endl;
    for (const auto &sql : thd->query_buffer.get_all()) {
      thread_log << sql << std::endl;
    }
  }

  if (thread_log.is_open())
    thread_log.close();

  if (client_log.is_open())
    client_log.close();

  delete thd;
}

bool Thd1::tryreconnet() {
  auto myParams = *this->myParam;
  return db->connect(myParams);
}
