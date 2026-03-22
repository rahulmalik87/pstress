/*
 =========================================================
 #       Created by Alexey Bychko, Percona LLC           #
 #     Expanded by Roel Van de Paar, Percona LLC         #
 =========================================================
*/

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common.hpp"
#include "node.hpp"
#include "pstress.hpp"
#include "random_test.hpp"
extern std::vector<Table *> *all_tables;
#ifdef USE_CLICKHOUSE
#include "ch_verify.hpp"
#endif
#include <INIReader.hpp>
#include <cstdlib>  // For free()
#include <cxxabi.h> // For demangling
#include <execinfo.h> //For backtrace()
#include <iostream>
#include <libgen.h> //dirname() uses this
#include <signal.h> //For signal()
#include <algorithm>
#include <filesystem>
#include <set>
#include <string>
#include <thread>

extern std::atomic<bool> run_query_failed;
thread_local std::mt19937 rng;

/* Scan logdir for existing metadata step files and return the next step to run.
   File pattern: {instance}_{db}_metadata_step_{N}.log
   Returns the highest N found + 1, or 1 if no files found. */
static int detect_next_step(const std::string &logdir, const std::string &db) {
#ifdef USE_MYSQL
  const std::string instance = "mysql";
#elif USE_DUCKDB
  const std::string instance = "duckdb";
#else
  const std::string instance = "clickhouse";
#endif
  const std::string prefix = instance + "_" + db + "_metadata_step_";
  const std::string suffix = ".log";
  int max_step = 0;
  try {
    for (const auto &entry : std::filesystem::directory_iterator(logdir)) {
      if (!entry.is_regular_file())
        continue;
      std::string fname = entry.path().filename().string();
      if (fname.rfind(prefix, 0) != 0)
        continue;
      if (fname.size() <= prefix.size() + suffix.size())
        continue;
      std::string num = fname.substr(prefix.size(),
                                     fname.size() - prefix.size() - suffix.size());
      if (num.empty() || !std::all_of(num.begin(), num.end(), ::isdigit))
        continue;
      max_step = std::max(max_step, std::stoi(num));
    }
  } catch (...) {}
  return max_step + 1;
}

void read_section_settings(struct workerParams *wParams, std::string secName,
                           std::string confFile) {
  INIReader reader(confFile);
  wParams->myName = secName;
  wParams->socket = reader.Get(secName, "socket", "");
  wParams->address = reader.Get(secName, "address", "localhost");
  wParams->username = reader.Get(secName, "user", "test");
  wParams->password = reader.Get(secName, "password", "");
  wParams->database = reader.Get(secName, "database", "");
  wParams->port = reader.GetInteger(secName, "port", 3306);
  wParams->threads = reader.GetInteger(secName, "threads", 10);
  wParams->queries_per_thread =
      reader.GetInteger(secName, "queries-per-thread", 10000);
  /*
#ifdef MAXPACKET
  wParams->maxpacket =
      reader.GetInteger(secName, "max-packet-size", MAX_PACKET_DEFAULT);
#endif
*/
  wParams->infile = reader.Get(secName, "infile", "pquery.sql");
  wParams->logdir = reader.Get(secName, "logdir", "/tmp");
}
void create_worker(struct workerParams *Params) {
  Node newNode;
  newNode.setAllParams(Params);
  newNode.startWork();
  newNode.end_node();
}

void crashHandler(int sig) {
  void *stack[64];
  int size = backtrace(stack, 64);
  char **symbols = backtrace_symbols(stack, size);

  std::cerr << "Crash! Signal: " << sig << " (";
  if (sig == SIGSEGV)
    std::cerr << "Segmentation Fault";
  else if (sig == SIGABRT)
    std::cerr << "Abort";
  else if (sig == SIGFPE)
    std::cerr << "Floating-Point Exception";
  else if (sig == SIGILL)
    std::cerr << "Illegal Instruction";
  else
    std::cerr << "Unknown";
  std::cerr << ")\nStack:\n";

  for (int i = 0; i < size; i++) {
    const char *symbol = symbols[i] ? symbols[i] : "??";
    int status;
    char *demangled = abi::__cxa_demangle(symbol, nullptr, nullptr, &status);
    std::cerr << "[" << i << "] " << (status == 0 ? demangled : symbol) << "\n";
    free(demangled);
  }

  free(symbols);
  _exit(1);
}
int main(int argc, char *argv[]) {
  // Register handler for SIGSEGV and other relevant signals
  signal(SIGSEGV, crashHandler); // Segmentation fault
  signal(SIGABRT, crashHandler); // Abort (e.g., from assert)
  signal(SIGFPE, crashHandler);  // Floating-point exception
  signal(SIGILL, crashHandler);  // Illegal instruction

  std::cout << "Command: ";
  for (int i = 0; i < argc; i++)
    std::cout << argv[i] << " ";
  std::cout << std::endl;

  std::vector<std::thread> nodes;
  add_options();
  int c;
  while (true) {
    struct option long_options[Option::MAX];
    int option_index = 0;
    int i = 0;
    for (auto op : *options) {
      if (op == nullptr)
        continue;
      long_options[i++] = {op->getName(), op->getArgs(), 0, op->getOption()};
    };
    long_options[i] = {0, 0, 0, 0};
    c = getopt_long_only(argc, argv, "c:d:a:i:l:s:p:u:P:t:q:vAEFNLDTNOSk",
                         long_options, &option_index);
    if (c == -1) {
      break;
      exit(EXIT_FAILURE);
    }

    switch (c) {
    case 'I':
      show_config_help();
      exit(EXIT_FAILURE);
    case 'C':
      show_cli_help();
      exit(EXIT_FAILURE);
      break;
    case Option::MYSQLD_SERVER_OPTION:
      std::cout << optarg << std::endl;
      add_server_options(optarg);
      break;
    case Option::SERVER_OPTION_FILE:
      add_server_options_file(optarg);
      break;
    case Option::INVALID_OPTION:
      std::cout << "Invalid option , exiting" << std::endl;
      exit(EXIT_FAILURE);
      break;
    default:
      if (c >= Option::MAX) {
        break;
      }
      if (options->at(c) == nullptr) {
        throw std::runtime_error("INVALID OPTION at line " +
                                 std::to_string(__LINE__) + " file " +
                                 __FILE__);
        break;
      }
      auto op = options->at(c);
      /* set command line */
      op->set_cl();
      if (op->getArgs() == required_argument) {
        switch (op->getType()) {
        case Option::INT:
          op->setInt(optarg);
          break;
        case Option::STRING:
          op->setString(optarg);
          break;
        case Option::BOOL:
          op->setBool(optarg);
          break;
        case Option::FLOAT:
          op->setFloat(optarg);
          break;
        }
      } else if (op->getArgs() == no_argument) {
        op->setBool(true);
      } else if (op->getArgs() == optional_argument) {
        op->setBool(true);
        if (optarg)
          op->setString(optarg);
        break;
      }
    }
  } // while
  auto initial_seed = opt_int(INITIAL_SEED);
  if (initial_seed == 0) {
    // Generate a random seed using a random device
    std::random_device rd;
    initial_seed = rd() ^ static_cast<unsigned int>(std::time(nullptr));
    std::cout << "Generated random seed: " << initial_seed << std::endl;
    options->at(Option::INITIAL_SEED)->setInt(initial_seed);
  }
  initial_seed += options->at(Option::STEP)->getInt();
  rng = std::mt19937(initial_seed);

  /* check if user has asked for help */
  if (options->at(Option::HELP)->getBool() == true) {
      show_help("verbose");
    delete_options();
    exit(0);
  }

  if (!options->at(Option::OPTION_PROB_FILE)->getString().empty()) {
    read_option_prob_file(options->at(Option::OPTION_PROB_FILE)->getString());
  }

#ifdef USE_DUCKDB
  // if step=1 or prepare=true remove the duckdb file in logdir
  if (options->at(Option::STEP)->getInt() == 1 ||
      options->at(Option::PREPARE)->getBool() == true) {
    std::cout << "Will recreate duckdb file" << std::endl;
    std::string logdir = options->at(Option::LOGDIR)->getString();
    std::string duckdb_file = logdir + "/duckdb";
    if (remove(duckdb_file.c_str()) != 0) {
      std::cerr << "Error deleting file " << duckdb_file << std::endl;
    }
  }
#endif

  auto confFile = options->at(Option::CONFIGFILE)->getString();

#ifdef USE_CLICKHOUSE
  /* Apply ClickHouse defaults early — ch_verify_startup() runs before worker
     threads call sum_of_all_options(), so defaults must be set here too. */
  if (options->at(Option::ADDRESS)->getString().empty())
    options->at(Option::ADDRESS)->setString("127.0.0.1");
  if (options->at(Option::USER)->getString() == "root")
    options->at(Option::USER)->setString("default");
  if (options->at(Option::DATABASE)->getString() == "test")
    options->at(Option::DATABASE)->setString("test_db");
#endif

  /* Auto-detect step from logdir if --step was not explicitly given.
     Scans for the highest existing metadata step file and runs the next one. */
  if (!options->at(Option::STEP)->cl) {
    const std::string &logdir = options->at(Option::LOGDIR)->getString();
    const std::string &db    = options->at(Option::DATABASE)->getString();
    int next_step = detect_next_step(logdir, db);
    options->at(Option::STEP)->setInt(next_step);
    std::cout << "Auto-detected step " << next_step
              << " (scanned " << logdir << ")" << std::endl;
  }

  auto ports = splitStringToArray<int>(options->at(Option::PORT)->getString());
  auto addrs = splitStringToArray<std::string>(options->at(Option::ADDRESS)->getString());
  if (addrs.empty()) {
#ifdef USE_CLICKHOUSE
    addrs.push_back("127.0.0.1");
#else
    addrs.push_back("localhost");
#endif
  }
  if (addrs.size() > 1 && addrs.size() != ports.size()) {
    std::cerr << "Error: --address has " << addrs.size()
              << " entries but --port has " << ports.size()
              << ". Provide one address (broadcast) or one per port.\n";
    exit(EXIT_FAILURE);
  }
#ifdef USE_MYSQL
  std::string name = "mysql";
#elif USE_DUCKDB
  std::string name = "duckdb";
#elif USE_CLICKHOUSE
  std::string name = "clickhouse";
#endif
  if (confFile.empty() && ports.size() == 1) {
    /*single node and command line */
    workerParams *wParams = new workerParams(ports[0]);
    wParams->myName = name;
    create_worker(wParams);
    delete wParams;
  } else if (confFile.empty() && ports.size() > 1) {
#ifdef USE_CLICKHOUSE
    /* Verify replicas are consistent before starting workload */
    ch_verify_startup(addrs, ports,
                      options->at(Option::DATABASE)->getString(),
                      options->at(Option::USER)->getString(),
                      options->at(Option::PASSWORD)->getString());
#endif
    for (size_t i = 0; i < ports.size(); i++) {
      workerParams *wParams = new workerParams(ports[i], i, ports.size());
      wParams->address = (addrs.size() == 1) ? addrs[0] : addrs[i];
      wParams->myName = name + "." + wParams->address + "." + std::to_string(ports[i]);
      nodes.push_back(std::thread(create_worker, wParams));
    }
#ifdef USE_CLICKHOUSE
    /* Periodic replica verification thread: pauses workers, checksums, resumes */
    std::atomic<bool> nodes_done(false);
    int verify_interval = options->at(Option::CH_VERIFY_INTERVAL)->getInt();
    std::thread verifier_thread;
    if (verify_interval > 0) {
      verifier_thread = std::thread([&]() {
        while (!nodes_done.load(std::memory_order_relaxed)) {
          for (int s = 0; s < verify_interval; s++) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            if (nodes_done.load(std::memory_order_relaxed)) return;
          }
          ch_verify_replicas(addrs, ports,
                             options->at(Option::DATABASE)->getString(),
                             options->at(Option::USER)->getString(),
                             options->at(Option::PASSWORD)->getString(),
                             {});
        }
      });
    }
#endif
    /* join all nodes */
    for (auto node = nodes.begin(); node != nodes.end(); node++)
      node->join();
#ifdef USE_CLICKHOUSE
    nodes_done.store(true, std::memory_order_relaxed);
    if (verifier_thread.joinable()) verifier_thread.join();
#endif
  } else {
    INIReader reader(confFile);
    if (reader.ParseError() < 0) {
      std::cout << "Can't load " << confFile << std::endl;
      exit(1);
    }

    auto sections = reader.GetSections();
    for (auto it = sections.begin(); it != sections.end(); it++) {
      std::string secName = *it;
      std::cerr << ": Processing config file for " << secName << std::endl;
      if (reader.GetBoolean(secName, "run", false)) {
        workerParams *wParams = new workerParams;
        read_section_settings(wParams, secName, confFile);
        nodes.push_back(std::thread(create_worker, wParams));
      }
    }

    /* join all nodes */
    for (auto node = nodes.begin(); node != nodes.end(); node++)
      node->join();
  }

  save_metadata_to_file();

#ifdef USE_CLICKHOUSE
  {
    const std::string &chdb  = options->at(Option::DATABASE)->getString();
    const std::string &chuser = options->at(Option::USER)->getString();
    const std::string &chpass = options->at(Option::PASSWORD)->getString();

    if (ports.size() > 1) {
      /* Collect table names before clean_up_at_end() frees all_tables */
      std::vector<std::string> tnames;
      std::set<std::string> seen;
      for (auto *t : *all_tables)
        if (seen.insert(t->name_).second)
          tnames.push_back(t->name_);

      ch_verify_replicas(addrs, ports, chdb, chuser, chpass, tnames);
    }

    /* Always verify schema (metadata vs actual ClickHouse columns) */
    ch_verify_schema(addrs, ports, chdb, chuser, chpass);
  }
#endif

  clean_up_at_end();

  /* print option with total_queries */
  for (auto op : *options) {
    if (op != nullptr && op->sql && op->total_queries > 0) {
      std::cout << op->short_help << ", total=>" << op->total_queries
                << ", success=>" << op->success_queries << std::endl;
    }
  }
  delete_options();
  std::cout << "COMPLETED" << std::endl;
  if (run_query_failed)
    return EXIT_FAILURE;
  else
    return EXIT_SUCCESS;
}

std::set<int> splitStringToIntSet(const std::string &input) {
  std::set<int> result;
  std::istringstream iss(input);
  std::string token;

  while (getline(iss, token, ',')) {
    int value;
    std::istringstream(token) >> value;
    result.insert(value);
  }

  return result;
}
