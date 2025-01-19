#include "common.hpp"
#include "node.hpp"
#include "random_test.hpp"
#include "ring_buffer.hpp"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <random>
#include <sstream>
std::atomic<bool> metadata_loaded(false);
std::atomic<int> thread_started(0);

inline unsigned long long Node::getAffectedRows(MYSQL *connection) {
  if (mysql_affected_rows(connection) == ~(unsigned long long)0) {
    return 0LL;
  }
  return mysql_affected_rows(connection);
}

void Node::workerThread(int number) {

  std::ofstream thread_log;
  std::ofstream client_log;
  if (options->at(Option::LOG_CLIENT_OUTPUT)->getBool()) {
    std::ostringstream cl;
    cl << myParams.logdir << "/" << myParams.myName << "_step_"
       << std::to_string(options->at(Option::STEP)->getInt()) << "_thread-"
       << number << ".out";
    client_log.open(cl.str(), std::ios::out | std::ios::trunc);
    if (!client_log.is_open()) {
      general_log << "Unable to open logfile for client output " << cl.str()
                  << ": " << std::strerror(errno) << std::endl;
      return;
    }
  }

  std::ostringstream os;
  os << myParams.logdir << "/" << myParams.myName << "_step_"
     << std::to_string(options->at(Option::STEP)->getInt()) << "_thread-"
     << number << ".sql";
  thread_log.open(os.str(), std::ios::out | std::ios::trunc);
  if (!thread_log.is_open()) {
    general_log << "Unable to open thread logfile " << os.str() << ": "
                << std::strerror(errno) << std::endl;
    return;
  }

  if (options->at(Option::LOG_QUERY_DURATION)->getBool()) {
    thread_log.precision(3);
    thread_log << std::fixed;
    std::cerr.precision(3);
    std::cerr << std::fixed;
    std::cout.precision(3);
    std::cout << std::fixed;
  }

  MYSQL *conn;

  conn = mysql_init(NULL);
  if (conn == NULL) {
    thread_log << "Error " << mysql_errno(conn) << ": " << mysql_error(conn)
               << std::endl;

    if (thread_log) {
      thread_log.close();
    }
    general_log << ": Thread #" << number << " is exiting abnormally"
                << std::endl;
    return;
  }
  /*
#ifdef MAXPACKET
  if (myParams.maxpacket != MAX_PACKET_DEFAULT) {
    mysql_options(conn, MYSQL_OPT_MAX_ALLOWED_PACKET, &myParams.maxpacket);
  }
#endif
*/
  if (mysql_real_connect(conn, myParams.address.c_str(),
                         myParams.username.c_str(), myParams.password.c_str(),
                         myParams.database.c_str(), myParams.port,
                         myParams.socket.c_str(), 0) == NULL) {
    std::cout << "Error " << mysql_errno(conn) << ": " << mysql_error(conn)
              << std::endl;
    mysql_close(conn);
    exit(EXIT_FAILURE);

    if (thread_log.is_open()) {
      thread_log.close();
    }
    mysql_thread_end();
    return;
  }

  Thd1 *thd = new Thd1(number, thread_log, general_log, client_log, conn,
                       performed_queries_total, failed_queries_total,
                       options->at(Option::N_LAST_QUERIES)->getInt());

  thd->myParam = &myParams;
  thread_started++;

  /* run pstress in with dynamic generator or infile */
  if (options->at(Option::PQUERY)->getBool() == false) {
    static bool success = false;

    /* load metadata */
    if (thread_started == options->at(Option::THREADS)->getInt()) {
      success = thd->load_metadata();
      metadata_loaded = true;
    }

    /* wait untill metadata is finished */
    while (!metadata_loaded) {
      std::chrono::seconds dura(3);
      std::this_thread::sleep_for(dura);
      thread_log << "waiting for metadata load to finish" << std::endl;
    }

    if (!success)
      thread_log << " initial setup failed, check logs for details "
                 << std::endl;
    else {
      auto result = thd->run_some_query();
      if (thd->query_buffer.size() > 0) {
        thread_log << "last N SQL executed by thread " << std::endl;
        for (const auto &sql : thd->query_buffer.get_all()) {
          thread_log << sql << std::endl;
        }
      }

      if (!result) {
        std::ostringstream errmsg;
        errmsg << "Thread with id " << thd->thread_id
               << " failed, check logs  in " << myParams.logdir << "/*sql";
        std::cerr << errmsg.str() << std::endl;
        exit(EXIT_FAILURE);
        if (general_log.is_open()) {
          general_log << errmsg.str() << std::endl;
        }
      }
    }

  } else {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(0, querylist->size() - 1);
    int max_con_failures = 250;
    for (unsigned long i = 0; i < myParams.queries_per_thread; i++) {
      unsigned long query_number;
      // selecting query #, depends on random or sequential execution
      if (options->at(Option::NO_SHUFFLE)->getBool()) {
        query_number = i;
      } else {
        query_number = dis(gen);
      }
      // perform the query and getting the result
      execute_sql((*querylist)[query_number].c_str(), thd);

      if (thd->max_con_fail_count >= max_con_failures) {
        std::ostringstream errmsg;
        errmsg << "* Last " << thd->max_con_fail_count
               << " consecutive queries all failed. Likely crash/assert, user "
                  "privileges drop, or similar. Ending run.";
        std::cerr << errmsg.str() << std::endl;
        if (thread_log.is_open()) {
          thread_log << errmsg.str() << std::endl;
        }
        break;
      }
    }
  }
  /* connection can be changed if we thd->tryreconnect is called */
  conn = thd->conn;
  delete thd;

  if (thread_log.is_open())
    thread_log.close();

  if (client_log.is_open())
    client_log.close();

  mysql_close(conn);
  mysql_thread_end();
}

bool Thd1::tryreconnet() {
  MYSQL *conn;
  auto myParams = *this->myParam;
  conn = mysql_init(NULL);
  if (mysql_real_connect(conn, myParams.address.c_str(),
                         myParams.username.c_str(), myParams.password.c_str(),
                         myParams.database.c_str(), myParams.port,
                         myParams.socket.c_str(), 0) == NULL) {
    std::cout << "Error Failed to reconnect " << mysql_errno(conn) << std::endl;
    mysql_close(conn);
    exit(EXIT_FAILURE);

    return false;
  }
  MYSQL *old_conn = this->conn;
  mysql_close(old_conn);
  this->conn = conn;
  return true;
}
